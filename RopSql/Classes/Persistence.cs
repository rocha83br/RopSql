using System;
using System.Xml;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Configuration;
using System.Data.RopSql;
using System.Data.RopSql.Interfaces;
using System.Data.RopSql.Resources;
using System.Data.RopSql.DataAnnotations;

namespace System.Data.RopSql
{
    public class DataPersistence : DataBaseOleDbConnection, IPersistence
    {
        #region Declarations

	    private readonly string[] emptyArray = new string[0];
        bool keepConnection = false;

        #endregion

        #region Constructors

        public DataPersistence() { }

        public DataPersistence(bool keepDBConnected)
        {
            keepConnection = keepDBConnected;
            base.connect();
        }

        #endregion

        #region Public Methods

        public int Create(object entity, Type entityType, bool persistComposition)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> commandParameters;
            int lastInsertedId = 0;
            List<string> childEntityCommands = null;

            if (keepConnection || base.connect())
            {
                sqlInstruction = parseEntity(Convert.ChangeType(entity, entityType), 
                                                  entityType,
                                                  (int)PersistenceAction.Create, 
                                                  null, null,
                                                  false,
                                                  emptyArray,
                                                  out commandParameters);
                 
                lastInsertedId = base.executeCommand(sqlInstruction, commandParameters);

                if (persistComposition)
                {
                    var entityColumnKey = getKeyColumn(entity, false);

                    if (entityColumnKey != null)
                        entityColumnKey.SetValue(entity, lastInsertedId, null);

                    childEntityCommands = traduzirComposicao(entity, entityType, (int)PersistenceAction.Create, null);

                    foreach (var cmd in childEntityCommands)
                        executeCommand(cmd, commandParameters);
                }
                
                if (!keepConnection) base.disconnect();
            }

            return lastInsertedId;
		}

        public int Edit(object entity, object filterEntity, Type entityType, bool persistComposition)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> commandParameters = null;
            int recordsAffected = 0;
            List<string> childEntityCommands = new List<string>();

            if (keepConnection || base.connect())
            {
                sqlInstruction = parseEntity(Convert.ChangeType(entity, entityType),
                                                     entityType, (int)PersistenceAction.Edit, 
                                                     filterEntity, null, false, emptyArray, out commandParameters);

                recordsAffected = executeCommand(sqlInstruction, commandParameters);
            }

            if (persistComposition)
            {
                childEntityCommands = traduzirComposicao(entity, entityType, (int)PersistenceAction.Edit, filterEntity);

                foreach (var cmd in childEntityCommands)
                    recordsAffected += executeCommand(cmd, commandParameters);
            }

            if (!keepConnection) base.disconnect();

            return recordsAffected;
		}

        public int Delete(object filterEntity, Type entityType)
        {
            string sqlInstruction = string.Empty;
            Dictionary<object, object> commandParameters;
            int recordAffected = 0;

            if (keepConnection || base.connect())
            {
                sqlInstruction = parseEntity(Convert.ChangeType(filterEntity, entityType),
                                             entityType,
                                             (int)PersistenceAction.Delete,
                                             Convert.ChangeType(filterEntity, entityType),
                                             null, false, emptyArray,
                                             out commandParameters);

                recordAffected = executeCommand(sqlInstruction, commandParameters);
            }

            if (!keepConnection) base.disconnect();

            return recordAffected;
		}

        public object View(object filterEntity, Type entityType, List<int> primaryKeyFilters, bool loadComposition)
        {
            object returnEntity = null;
            
            var queryList = List(filterEntity, entityType, primaryKeyFilters, 0, 
                                       string.Empty, string.Empty, string.Empty, 
                                       false, false, false, true, loadComposition);

            if (queryList.Count > 0) returnEntity = queryList[0];
            
            return returnEntity;
		}

        public IList List(object filterEntity, Type entityType, List<int> primaryKeyFilters, int recordLimit, string showAttributes, string groupAttributes, string orderAttributes, bool onlyListableAttributes, bool getExclusion, bool orderDescending, bool uniqueQuery, bool loadComposition)
        {
            string sqlInstruction = string.Empty;
            string[] displayAttributes = new string[0];
            string[] groupingAttributes = new string[0];
            string[] ordinationAttributes = new string[0];
            Dictionary<object, object> attributeColumnRelation = null;
            Dictionary<object, object> commandParameters;
            string columnList = string.Empty;
            int persistenceAction = uniqueQuery ? (int)PersistenceAction.View 
                                                : (int)PersistenceAction.List;

            Type dynamicListType = typeof(List<>).MakeGenericType(new Type[] { entityType });
            object returnList = Activator.CreateInstance(dynamicListType, true);

            if (onlyListableAttributes)
                validateListableAttributes(entityType, showAttributes, out displayAttributes);

            // Montando instrução de consulta
            
            sqlInstruction = parseEntity(Convert.ChangeType(filterEntity, entityType), 
                                         entityType,
                                         persistenceAction, 
                                         Convert.ChangeType(filterEntity, entityType), 
                                         primaryKeyFilters, getExclusion, displayAttributes,
                                         out commandParameters);

            sqlInstruction = string.Format(sqlInstruction, recordLimit > 0 ? string.Format(SQLANSIRepository.DataPersistence_Action_LimitResult_MySQL, recordLimit) : string.Empty, "{0}", "{1}");

            if (!string.IsNullOrEmpty(groupAttributes))
            {
                string complementaryColumnList = string.Empty;

                groupingAttributes = groupAttributes.Split(',');

                for (int cont = 0; cont < groupingAttributes.Length; cont++)
                    groupingAttributes[cont] = groupingAttributes[cont].Trim();

                attributeColumnRelation = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, persistenceAction, null, out commandParameters);

                foreach (var rel in attributeColumnRelation)
                    if (Array.IndexOf(groupingAttributes, rel.Key) > -1)
                        columnList += string.Format("{0}, ", ((KeyValuePair<string, object>)rel.Value).Key);
                    else
                        if ((!rel.Key.Equals("Class")) && (!rel.Key.Equals("DataTable")))
                            complementaryColumnList += string.Format("{0}, ", ((KeyValuePair<string, object>)rel.Value).Key);

                if (!String.IsNullOrEmpty(columnList) && Convert.ToInt32(columnList) > 2)
                    columnList = columnList.Substring(0, columnList.Length - 2);
                if (!String.IsNullOrEmpty(complementaryColumnList) && Convert.ToInt32(complementaryColumnList) > 2)
                    complementaryColumnList = complementaryColumnList.Substring(0, complementaryColumnList.Length - 2);

                sqlInstruction = string.Format(sqlInstruction,
                                              string.Format(SQLANSIRepository.DataPersistence_Action_Group,
                                                            columnList, ", ", complementaryColumnList),
                                                            "{0}");
            }
            else
                sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

            if (!string.IsNullOrEmpty(orderAttributes))
            {
                ordinationAttributes = orderAttributes.Split(',');
                attributeColumnRelation = getAnnotationValueList(Convert.ChangeType(filterEntity, entityType), entityType, persistenceAction, null, out commandParameters);

                for (int contAtrib = 0; contAtrib < ordinationAttributes.Length; contAtrib++)
                {
                    ordinationAttributes[contAtrib] = ordinationAttributes[contAtrib].Trim();

                    var attribToOrder = attributeColumnRelation.FirstOrDefault(rca => ordinationAttributes[contAtrib].Equals(rca.Key));
                    var columnToOrder = ((KeyValuePair<object, object>)attribToOrder.Value).Key;
                    
                    if (!(columnToOrder is RelationalColumn))
                        columnList = string.Concat(columnList, columnToOrder, ", ");
                    else
                        columnList = string.Concat(columnList, string.Format("{0}.{1}", ((RelationalColumn)columnToOrder).TableName.ToLower(),
                                                                                        ((RelationalColumn)columnToOrder).ColumnName), ", ");
                }

                columnList = columnList.Substring(0, columnList.Length - 2);

                sqlInstruction = string.Format(sqlInstruction,
                                               string.Format(SQLANSIRepository.DataPersistence_Action_OrderResult,
                                                             columnList,
                                                             orderDescending ? "DESC" : "ASC"));
            }
            else
                sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

            if (keepConnection || base.connect())
            {
                // Tratando retorno do banco

                XmlDocument queryReturn = base.executeQuery(sqlInstruction);

                returnList = parseDatabaseReturn(queryReturn, filterEntity.GetType());
            }

            if(!keepConnection) base.disconnect();

            // Efetuando carga da composição quando existente (Eager Loading)

            if (loadComposition && (((IList)returnList).Count > 0))
                for (int inC = 0; inC < ((IList)returnList).Count; inC++)
                    fillComposition(((IList)returnList)[inC], ((IList)returnList)[inC].GetType());

            return (IList)returnList;
		}

        public void SetSearchFilter(object entity, string filter)
        {
            DateTime datetimeValue;
            int numericValue;
            long longNumericValue;

            if (!string.IsNullOrEmpty(filter))
            {
                var filterableAttributes = entity.GetType().GetProperties()
                                                           .Where(atb => atb.GetCustomAttributes(true)
                                                           .Any(ca => (ca is IDataColumn)
                                                                   && ((IDataColumn)ca).IsFilterable()));

                foreach (PropertyInfo attrib in filterableAttributes)
                {
                    if (attrib.PropertyType.Name.Equals("DateTime")
                        && DateTime.TryParse(filter, out datetimeValue))
                        attrib.SetValue(entity, datetimeValue, null);
                    else if (attrib.PropertyType.Name.Equals("Int32")
                        && int.TryParse(filter, out numericValue))
                        attrib.SetValue(entity, numericValue, null);
                    else if (attrib.PropertyType.Name.Equals("Int64")
                        && long.TryParse(filter, out longNumericValue))
                        attrib.SetValue(entity, longNumericValue, null);
                    else if (attrib.PropertyType.Name.Equals("String"))
                        attrib.SetValue(entity, filter, null);
                }
            }
        }

        #endregion

        #region Helper Methods

        private string parseEntity(object entidade, Type tipoEntidade, int acao, object entidadeFiltro, List<int> filtrosChavePrimaria, bool obterExclusao, string[] atributosExibir, out Dictionary<object, object> parametrosComando)
        {
            string instrucaoSQL = string.Empty;
            Dictionary<object, object> dadosSQLFiltro;

            parametrosComando = null;

            Dictionary<object, object> dadosSQLEntidade = obterListaAnotacoesValores(Convert.ChangeType(entidade, tipoEntidade), tipoEntidade, acao, filtrosChavePrimaria, out parametrosComando);

            if (entidadeFiltro != null)
                dadosSQLFiltro = obterListaAnotacoesValores(Convert.ChangeType(entidadeFiltro, tipoEntidade), tipoEntidade, acao, filtrosChavePrimaria, out parametrosComando);
            else
                dadosSQLFiltro = null;

            var colunaChave = obterColunaChave(entidade, false).GetCustomAttributes(true)
                                                               .FirstOrDefault(cln => cln is ColunaDados 
                                                                                   && ((IColunaDados)cln).EhChavePrimaria()) as ColunaDados;
            string nomeColunaChave = string.Empty;
            if (colunaChave != null)
                nomeColunaChave = colunaChave.NomeColuna;

            var hashEntidade = tipoEntidade.GetCustomAttributes(true)
                                           .FirstOrDefault(cln => cln is AssinaturaHash) as AssinaturaHash;
            var codigoHash = hashEntidade != null ? hashEntidade.CodigoHash : 0;

            Dictionary<string, string> parametrosSQL = obterParametrosSQL(dadosSQLEntidade, acao, dadosSQLFiltro, atributosExibir, nomeColunaChave, codigoHash, (filtrosChavePrimaria != null), obterExclusao);

            switch (acao)
            {
                case (int)AcaoPersistencia.Inclusao:

                    instrucaoSQL = String.Format(RepositorioSQL.PersistenciaDados_Acao_Inserir,
                                                 parametrosSQL["Tabela"].ToLower(),
                                                 parametrosSQL["listaCampos"],
                                                 parametrosSQL["listaValores"]);

                    break;

                case (int)AcaoPersistencia.Alteracao:

                    instrucaoSQL = String.Format(RepositorioSQL.PersistenciaDados_Acao_Alterar,
                                                 parametrosSQL["Tabela"].ToLower(),
                                                 parametrosSQL["listaCamposComValores"],
                                                 parametrosSQL["listaCamposFiltro"]);

                    break;

                case (int)AcaoPersistencia.Exclusao:

                    instrucaoSQL = String.Format(RepositorioSQL.PersistenciaDados_Acao_Excluir,
                                                 parametrosSQL["Tabela"].ToLower(),
                                                 parametrosSQL["listaCamposFiltro"]);

                    break;
                default: // Listagem ou Consulta

                    instrucaoSQL = String.Format(RepositorioSQL.PersistenciaDados_Acao_Consultar,
                                                 "{0}",
                                                 parametrosSQL["listaCampos"],
                                                 parametrosSQL["Tabela"].ToLower(),
                                                 parametrosSQL["listaRelacionamentos"],
                                                 parametrosSQL["listaCamposFiltro"],
                                                 "{1}", "{2}", string.Empty);

                    break;
            }

            return instrucaoSQL;
        }

        private List<string> traduzirComposicao(object entidade, Type tipoEntidade, int acao, object entidadeFiltro)
        {
            List<string> resultado = new List<string>();
            object instanciaEntidadeFilho = null;
            Dictionary<object, object> parametrosComando;

            IEnumerable<PropertyInfo> entidadesFilho = tipoEntidade.GetProperties().Where(prp => prp.GetCustomAttributes(true)
                                                                   .Any(an => an.GetType().Name.Equals("EntidadeRelacionada")));

            foreach (PropertyInfo entidadeFilho in entidadesFilho)
            {
                instanciaEntidadeFilho = entidadeFilho.GetValue(entidade, null);
                object entidadeFilhoFiltro = null;

                var entidadePai = (acao != (int)AcaoPersistencia.Alteracao) ? entidade : entidadeFiltro;

                if (instanciaEntidadeFilho != null)
                {
                    if (!instanciaEntidadeFilho.GetType().Name.Contains("List"))
                    {
                        acao = definirAcaoPersistencia(instanciaEntidadeFilho, obterColunaChave(entidadeFilho, false));
                        entidadeFilhoFiltro = Activator.CreateInstance(instanciaEntidadeFilho.GetType());

                        if (acao == (int)AcaoPersistencia.Alteracao)
                            migrarChavePrimariaEntidade(instanciaEntidadeFilho, entidadeFilhoFiltro);

                        definirChaveHashEntidade(entidadePai, entidadeFilho);

                        resultado.Add(traduzirEntidade(instanciaEntidadeFilho, instanciaEntidadeFilho.GetType(), acao, entidadeFilhoFiltro, null, false, null, out parametrosComando));
                    }
                    else
                    {
                        var instanciaListaFilho = (IList)instanciaEntidadeFilho;
                        List<object> listaFiltrosFilho = new List<object>();

                        foreach (var itemListaFilho in instanciaListaFilho)
                        {
                            acao = definirAcaoPersistencia(itemListaFilho, obterColunaChave(itemListaFilho, false));
                            entidadeFilhoFiltro = Activator.CreateInstance(itemListaFilho.GetType());

                            if (acao == (int)AcaoPersistencia.Alteracao)
                            {
                                migrarChavePrimariaEntidade(itemListaFilho, entidadeFilhoFiltro);
                                listaFiltrosFilho.Add(entidadeFilhoFiltro);
                            }

                            definirChaveHashEntidade(entidadePai, itemListaFilho);

                            resultado.Add(traduzirEntidade(itemListaFilho, itemListaFilho.GetType(), acao, entidadeFilhoFiltro, null, false, null, out parametrosComando));
                        }

                        if (listaFiltrosFilho.Count > 0)
                            resultado.Add(obterComposicaoExcluir(entidadeFiltro, listaFiltrosFilho));
                    }
                }
            }

            if (resultado.Any(rst => rst.Contains(RepositorioSQL.PersistenciaDados_PalavraReservada_Insercao)))
                resultado.Reverse();

            return resultado;
        }

        private string obterComposicaoExcluir(object entidade, IList composicaoExistente)
        {
            List<int> chavesExistentes = new List<int>();
            AdaptadorDados filtroComposicao = null;

            if (composicaoExistente.Count > 0)
            {
                filtroComposicao = Activator.CreateInstance(composicaoExistente[0].GetType()) as AdaptadorDados;
                definirChaveHashEntidade(entidade, filtroComposicao);
            }

            foreach (var composicao in composicaoExistente)
            {
                var colunaChaveComposicao = obterColunaChave(composicao, false);
                chavesExistentes.Add(int.Parse(colunaChaveComposicao.GetValue(composicao, null).ToString()));
            }

            Dictionary<object, object> parametrosComando;
            
            string resultado = string.Empty;
            if (filtroComposicao != null)
                resultado = traduzirEntidade(filtroComposicao, filtroComposicao.GetType(), (int)AcaoPersistencia.Exclusao, filtroComposicao, chavesExistentes, true, null, out parametrosComando);

            return resultado;
        }

        private bool definirChaveHashEntidade(object entidadePai, object entidadeFilho)
        {
            bool resultado = false;
            AssinaturaHash anotacaoHashPai = null;

            var colunaChaveEntidade = obterColunaChave(entidadePai, false);

            if (colunaChaveEntidade != null)
            {
                var identReferencia = obterColunaChave(entidadeFilho, true);

                if (identReferencia != null)
                {
                    identReferencia.SetValue(entidadeFilho, colunaChaveEntidade.GetValue(entidadePai, null), null);
                    resultado = true;
                }
                else
                    resultado = false;
            }

            var colunaHashEntidade = entidadeFilho.GetType().GetProperties()
                                                  .FirstOrDefault(fd => fd.Name.Equals("HashTipoReferencia"));

            if (colunaHashEntidade != null)
            {
                anotacaoHashPai = entidadePai.GetType().GetCustomAttributes(true)
                                         .FirstOrDefault(ca => ca.GetType().Name
                                         .Equals("AssinaturaHash")) as AssinaturaHash;

                if (anotacaoHashPai != null)
                {
                    colunaHashEntidade.SetValue(entidadeFilho, anotacaoHashPai.CodigoHash, null);
                    resultado = resultado && true;
                }
            }

            return resultado;
        }

        private bool replicarHashFilhos(object entidadeCarregada, object instanciaAtributo)
        {
            bool resultado = false;
            List<int> identidadesAtributosGenericos = null;

            var atributoGenericoFiltro = instanciaAtributo.GetType()
                                                          .GetProperties()
                                                          .FirstOrDefault(atb => atb.PropertyType
                                                          .GetInterface("IHashTipoReferencia") != null);

            if (atributoGenericoFiltro != null)
            {
                var instanciaAtributoGenerico = Activator.CreateInstance(atributoGenericoFiltro.PropertyType, true);
                var colunaChaveRelacional = obterColunaChave(instanciaAtributo, false);

                definirChaveHashEntidade(entidadeCarregada, instanciaAtributoGenerico);

                var atributosGenericos = Listar(instanciaAtributoGenerico, instanciaAtributoGenerico.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, false);

                identidadesAtributosGenericos = new List<int>();

                foreach (var atributoGenerico in atributosGenericos)
                {
                    var colunaChaveAtributoGenerico = obterColunaChave(atributoGenerico, false);

                    identidadesAtributosGenericos.Add(int.Parse(
                    colunaChaveAtributoGenerico.GetValue(atributoGenerico, null).ToString()));

                    colunaChaveRelacional.SetValue(instanciaAtributo, int.Parse(ValoresPadraoCamposSql.Falso), null);
                }

                if (identidadesAtributosGenericos.Count > 0)
                    instanciaAtributo = Consultar(instanciaAtributo, instanciaAtributo.GetType(), identidadesAtributosGenericos, true);

                resultado = true;
            }

            return resultado;
        }

        private void migrarChavePrimariaEntidade(object entidade, object entidadeFiltro)
        {
            var colunaChaveEntidade = getKeyColumn(entidade, false);

            if (colunaChaveEntidade != null)
            {
                var valorChave = colunaChaveEntidade.GetValue(entidade, null);
                colunaChaveEntidade.SetValue(entidadeFiltro, valorChave, null);
                colunaChaveEntidade.SetValue(entidade, 0, null);
            }
        }

        private int setPersistenceAction(object entity, PropertyInfo entityKeyColumn)
        {
            return (entityKeyColumn.GetValue(entity, null).ToString().Equals(ValoresPadraoCamposSql.Zerado))
                    ? (int)PersistenceAction.Create : (int)PersistenceAction.Edit;
        }

        private IList parseDatabaseReturn(XmlDocument retornoBancoDados, Type tipoEntidade)
        {
            Type tipoDinamicoLista = typeof(List<>).MakeGenericType(new Type[] { tipoEntidade });
            object listaRetorno = Activator.CreateInstance(tipoDinamicoLista, true);
            object entidadeRetorno = null;
            XmlNodeList listaElementos;
            
            listaElementos = retornoBancoDados.GetElementsByTagName("Table");

            foreach (XmlNode elementoEntidade in listaElementos)
            {
                entidadeRetorno = Activator.CreateInstance(tipoEntidade, true);

                foreach (XmlNode elementoAtributo in elementoEntidade.ChildNodes)
                {
                    foreach (PropertyInfo campo in tipoEntidade.GetProperties())
                    {
                        if (campo.GetCustomAttributes(false).FirstOrDefault(ca => (ca is ColunaDados 
                                                                                      && ((ColunaDados)ca).NomeColuna.Equals(elementoAtributo.Name))
                                                                                      || (ca is ColunaRelacional 
                                                                                             && (((((ColunaRelacional)ca).NomeColuna.Equals(elementoAtributo.Name)
                                                                                                  && ((ColunaRelacional)ca).CodeNomeColuna == null))
                                                                                                 || (((ColunaRelacional)ca).CodeNomeColuna != null
                                                                                                 && ((ColunaRelacional)ca).CodeNomeColuna.Equals(elementoAtributo.Name))))) != null)
                            
                            campo.SetValue(entidadeRetorno, formatarValorSaidaCampoSQL(elementoAtributo.InnerText, campo.PropertyType), null);
                    }
                }

                ((IList)listaRetorno).Add(entidadeRetorno);
            }

            return (IList)listaRetorno;
        }

        private Dictionary<object, object> getAnnotationValueList(object entidade, Type tipoEntidade, int acao, List<int> filtrosChavePrimaria, out Dictionary<object, object> parametrosComando)
        {
            var associacaoDadosObjetoSQL = new Dictionary<object, object>();
            parametrosComando = new Dictionary<object, object>();

            associacaoDadosObjetoSQL.Add("Classe", tipoEntidade.Name);

            object[] anotacoesClasse = tipoEntidade.GetCustomAttributes(true);

            var anotacaoTabela = anotacoesClasse.FirstOrDefault(ant => ant.GetType().Name.Equals("Tabela"));
            if (anotacaoTabela != null)
                associacaoDadosObjetoSQL.Add("Tabela", ((Tabela)anotacaoTabela).NomeTabela.ToLower());

            PropertyInfo atributoChavePrimaria = obterColunaChave(entidade, false);

            PropertyInfo[] listaAtributos = tipoEntidade.GetProperties();

            foreach (var atributo in listaAtributos)
            {
                object[] anotacoesAtributo = atributo.GetCustomAttributes(true);

                foreach (object anotacao in anotacoesAtributo.Where(ca => ca is IColunaDados))
                {
                    object valorCampo = atributo.GetValue(Convert.ChangeType(entidade, tipoEntidade), null);
                    valorCampo = formatarValorEntradaCampoSQL(valorCampo, acao, (IColunaDados)anotacao, parametrosComando);

                    if (anotacao.GetType().Name.Equals("ColunaDados"))
                    {
                        object campoValorSQL = null;

                        if (!(acao == (int)AcaoPersistencia.Inclusao 
                            && ((ColunaDados)anotacao).AutoNumeracao))
                        {
                            if (filtrosChavePrimaria == null 
                                || ((filtrosChavePrimaria != null) && !((ColunaDados)anotacao).Filtravel))
                            {
                                campoValorSQL = new KeyValuePair<object, object>(((ColunaDados)anotacao).NomeColuna, valorCampo);
                                associacaoDadosObjetoSQL.Add(atributo.Name, campoValorSQL);
                            }
                            else
                            {
                                if (((ColunaDados)anotacao).Filtravel && ((ColunaDados)anotacao).MultiplosFiltros)
                                {
                                    string intervaloChaves = string.Empty;

                                    foreach (var filtroChave in filtrosChavePrimaria)
                                        intervaloChaves += string.Concat(filtroChave.ToString(), ",");

                                    campoValorSQL = new KeyValuePair<object, object>(
                                                    ((ColunaDados)anotacao).NomeColuna,
                                                    intervaloChaves.Substring(0, intervaloChaves.Length - 1));

                                    associacaoDadosObjetoSQL.Add(atributo.Name, campoValorSQL);
                                }
                            }
                        }
                    }
                    else if (anotacao.GetType().Name.Equals("ColunaRelacional")
                            && ((acao == (int)AcaoPersistencia.Listagem) || (acao == (int)AcaoPersistencia.Consulta)))
                    {
                        object campoValorSQL = new KeyValuePair<object, object>((ColunaRelacional)anotacao, valorCampo);
                        associacaoDadosObjetoSQL.Add(atributo.Name, campoValorSQL);
                    }
                }
            }

            return associacaoDadosObjetoSQL;
        }

        private Dictionary<string, string> obterParametrosSQL(Dictionary<object, object> dadosSQLEntidade, int acao, Dictionary<object, object> dadosSQLFiltro, string[] atributosExibir, string nomeColunaChave, long hashEntidade, bool filtrosMultiplos, bool obterExclusao)
        {
            var dicionarioRetorno = new Dictionary<string, string>();
            var dicionarioRelacionamentos = new Dictionary<string, string>();

            string _nomeTabela = string.Empty;
            string _listaCampos = string.Empty;
            string _listaValores = string.Empty;
            string _listaCamposComValores = string.Empty;
            string _listaCamposFiltro = string.Empty;
            string _listaRelacionamentos = string.Empty;
            string _relacionamento = string.Empty;

            foreach (var item in dadosSQLEntidade.Where(item => !item.Key.Equals("Classe")))
            {
                _relacionamento = string.Empty;

                if (item.Key.Equals("Tabela"))
                {
                    dicionarioRetorno.Add(item.Key.ToString(), item.Value.ToString());
                    _nomeTabela = item.Value.ToString().ToLower();
                }
                else if (((KeyValuePair<object, object>)item.Value).Key is ColunaRelacional)
                {
                    ColunaRelacional configRelacao = ((KeyValuePair<object, object>)item.Value).Key as ColunaRelacional;

                    _listaCampos += string.Format("{0}.{1} ", configRelacao.NomeTabela.ToLower(), configRelacao.NomeColuna);

                    if (!string.IsNullOrEmpty(configRelacao.CodeNomeColuna))
                        _listaCampos += string.Format(RepositorioSQL.PersistenciaDados_Acao_ApelidarColuna, configRelacao.CodeNomeColuna);

                    _listaCampos += ", ";

                    if (configRelacao.TipoJuncao == TipoJuncaoRelacional.Obrigatoria)
                    {
                        _relacionamento = string.Format(RepositorioSQL.PersistenciaDados_Acao_RelacionarObrigatoriamente,
                                                        configRelacao.NomeTabela.ToLower(),
                                                        string.Concat(_nomeTabela, ".", configRelacao.ColunaChave),
                                                        string.Concat(configRelacao.NomeTabela.ToLower(), ".", configRelacao.ColunaChaveEstrangeira));
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(configRelacao.NomeTabelaIntermediaria))
                        {
                            _relacionamento = string.Format(RepositorioSQL.PersistenciaDados_Acao_RelacionarOpcionalmente,
                                                             configRelacao.NomeTabelaIntermediaria.ToLower(),
                                                             string.Concat(_nomeTabela, ".", configRelacao.ColunaChaveEstrangeira),
                                                             string.Concat(configRelacao.NomeTabelaIntermediaria.ToLower(), ".", configRelacao.ColunaChaveIntermediaria));

                            _relacionamento += string.Format(RepositorioSQL.PersistenciaDados_Acao_RelacionarOpcionalmente,
                                                             configRelacao.NomeTabela.ToLower(),
                                                             string.Concat(configRelacao.NomeTabelaIntermediaria.ToLower(), ".", configRelacao.ColunaChave),
                                                             string.Concat(configRelacao.NomeTabela.ToLower(), ".", configRelacao.ColunaChaveEstrangeira));

                            if (configRelacao.RelacionaHash)
                                _relacionamento += string.Format(" AND NR_HASH_REFERENCIA = {0} ", hashEntidade);
                        }
                        else
                        {
                            _relacionamento = string.Format(RepositorioSQL.PersistenciaDados_Acao_RelacionarOpcionalmente,
                                                             configRelacao.NomeTabela.ToLower(),
                                                             string.Concat(_nomeTabela, ".", configRelacao.ColunaChaveEstrangeira),
                                                             string.Concat(configRelacao.NomeTabela.ToLower(), ".", configRelacao.ColunaChave));
                        }
                    }
                    
                    if (_relacionamento.Contains(_listaRelacionamentos)
                        || string.IsNullOrEmpty(_listaRelacionamentos))
                        _listaRelacionamentos = _relacionamento;
                    else if (!_listaRelacionamentos.Contains(_relacionamento))
                        _listaRelacionamentos += _relacionamento;
                }
                else if (item.Key.Equals("EntidadeRelacionada"))
                {

                }
                else
                {
                    string nomeAtributoEntidade = item.Key.ToString();
                    object nomeCampoEntidade = ((KeyValuePair<object, object>)item.Value).Key;
                    object valorCampoEntidade = ((KeyValuePair<object, object>)item.Value).Value;

                    switch (acao)
                    {
                        case (int)AcaoPersistencia.Inclusao:
                            _listaCampos += string.Format("{0}, ", nomeCampoEntidade);
                            _listaValores += string.Format("{0}, ", valorCampoEntidade);
                            
                            break;
                        case (int)AcaoPersistencia.Listagem:
                            if (atributosExibir.Length > 0)
                                for (int vC = 0; vC < atributosExibir.Length; vC++)
                                    atributosExibir[vC] = atributosExibir[vC].Trim();
                            if ((atributosExibir.Length == 0)
                                || atributosExibir.Length > 0 && Array.IndexOf(atributosExibir, nomeAtributoEntidade) > -1)
                                _listaCampos += string.Format("{0}.{1}, ", _nomeTabela, nomeCampoEntidade);
                            
                            break;
                        case (int)AcaoPersistencia.Consulta:
                            if (atributosExibir.Length > 0)
                                for (int vC = 0; vC < atributosExibir.Length; vC++)
                                    atributosExibir[vC] = atributosExibir[vC].Trim();
                            if ((atributosExibir.Length == 0)
                                || atributosExibir.Length > 0 && Array.IndexOf(atributosExibir, nomeAtributoEntidade) > -1)
                                _listaCampos += string.Format("{0}.{1}, ", _nomeTabela, nomeCampoEntidade);

                            break;
                        default: // Alteração e Exclusão
                            if ((valorCampoEntidade != null)
                                && (valorCampoEntidade.ToString() != ValoresPadraoCamposSql.Zerado)
                                && (valorCampoEntidade.ToString() != ValoresPadraoCamposSql.Nulo))
                                _listaCamposComValores += string.Format("{0} = {1}, ", nomeCampoEntidade, valorCampoEntidade);
                            
                            break;
                    }
                }
            }
            if (dadosSQLFiltro != null)
            {
                foreach (var itemFiltro in dadosSQLFiltro)
                {
                    if ((!itemFiltro.Key.Equals("Classe")) && (!itemFiltro.Key.Equals("Tabela"))
                        && (!itemFiltro.Key.Equals("EntidadeRelacionada")))
                    {
                        object nomeCampoFiltro = null;
                        object valorCampoFiltro = null;
                        object nomeCampo = null;

                        if (!(((KeyValuePair<object, object>)itemFiltro.Value).Key is ColunaRelacional))
                        {
                            nomeCampo = ((KeyValuePair<object, object>)itemFiltro.Value).Key;
                            nomeCampoFiltro = string.Concat(_nomeTabela, ".", nomeCampo);
                            valorCampoFiltro = ((KeyValuePair<object, object>)itemFiltro.Value).Value;
                        }
                        else
                        {
                            ColunaRelacional configRelacao = ((KeyValuePair<object, object>)itemFiltro.Value).Key as ColunaRelacional;

                            if ((acao == (int)AcaoPersistencia.Listagem) && configRelacao.Filtravel)
                            {
                                nomeCampoFiltro = string.Concat(configRelacao.NomeTabela.ToLower(), ".", configRelacao.NomeColuna);
                                valorCampoFiltro = ((KeyValuePair<object, object>)itemFiltro.Value).Value;
                            }
                        }

                        if ((valorCampoFiltro != null)
                                && (valorCampoFiltro.ToString() != ValoresPadraoCamposSql.Nulo)
                                && (valorCampoFiltro.ToString() != ValoresPadraoCamposSql.Zerado))
                        {
                            string comparacao = string.Empty;

                            if (!filtrosMultiplos)
                                comparacao = ((acao == (int)AcaoPersistencia.Listagem) && !nomeCampoFiltro.ToString().Contains("DT_"))
                                              ? string.Format(OperadoresSql.Contem, valorCampoFiltro.ToString().Replace("'", string.Empty))
                                              : string.Concat(OperadoresSql.Igual, valorCampoFiltro);
                            else
                            {
                                if (valorCampoFiltro.ToString().Contains(','))
                                {
                                    comparacao = string.Format(OperadoresSql.Em, valorCampoFiltro);
                                    if (obterExclusao) comparacao = string.Concat(OperadoresSql.Negacao, comparacao);
                                }
                                else
                                {
                                    if (!obterExclusao)
                                        comparacao = string.Concat(OperadoresSql.Igual, valorCampoFiltro);
                                    else
                                    {
                                        if (nomeCampo.Equals(nomeColunaChave))
                                            comparacao = string.Concat(OperadoresSql.Diferente, valorCampoFiltro);
                                        else
                                            comparacao = string.Concat(OperadoresSql.Igual, valorCampoFiltro);
                                    }
                                }
                            }

                            _listaCamposFiltro += nomeCampoFiltro + comparacao +
                                ((acao == (int)AcaoPersistencia.Listagem) ? OperadoresSql.Ou : OperadoresSql.E);
                        }
                    }
                }
            }

            if (acao == (int)AcaoPersistencia.Inclusao)
            {
                _listaCampos = _listaCampos.Substring(0, _listaCampos.Length - 2);
                _listaValores = _listaValores.Substring(0, _listaValores.Length - 2);

                dicionarioRetorno.Add("listaCampos", _listaCampos);
                dicionarioRetorno.Add("listaValores", _listaValores);
            }
            else
            {
                if ((acao == (int)AcaoPersistencia.Listagem)
                    || (acao == (int)AcaoPersistencia.Consulta))
                {
                    _listaCampos = _listaCampos.Substring(0, _listaCampos.Length - 2);
                    dicionarioRetorno.Add("listaCampos", _listaCampos);
                    dicionarioRetorno.Add("listaRelacionamentos", _listaRelacionamentos);
                }
                else
                    if (!string.IsNullOrEmpty(_listaCamposComValores))
                    {
                        _listaCamposComValores = _listaCamposComValores.Substring(0, _listaCamposComValores.Length - 2);

                        dicionarioRetorno.Add("listaCamposComValores", _listaCamposComValores);
                    }

                if (!string.IsNullOrEmpty(_listaCamposFiltro))
                {
                    var caracRemover = (acao == (int)AcaoPersistencia.Listagem)
                                       ? OperadoresSql.Ou.Length 
                                       : OperadoresSql.E.Length;

                    _listaCamposFiltro = _listaCamposFiltro.Substring(0, _listaCamposFiltro.Length - caracRemover);

                    dicionarioRetorno.Add("listaCamposFiltro", _listaCamposFiltro);
                }
                else
                    dicionarioRetorno.Add("listaCamposFiltro", "1 = 1");
            }

            return dicionarioRetorno;
        }

        public void fillComposition(object entidadeCarregada, Type tipoEntidade)
        {
            EntidadeRelacionada configRelacao = null;
            object instanciaAtributo = null;

            var listaAtributos = tipoEntidade.GetProperties()
                                             .Where(prp => prp.GetCustomAttributes(true)
                                                              .Any(atb => atb.GetType().Name.Equals("EntidadeRelacionada")));

            foreach(var atributo in listaAtributos)
            {
                IEnumerable<object> anotacoesAtributo = atributo.GetCustomAttributes(true)
                                                                .Where(atb => atb.GetType().Name.Equals("EntidadeRelacionada"));

                foreach (object anotacao in anotacoesAtributo)
                {
                    configRelacao = (EntidadeRelacionada)anotacao;

                    PropertyInfo colunaChaveEstrangeira = null;

                    PropertyInfo colunaChavePrimaria = obterColunaChave(entidadeCarregada, false);

                    switch (configRelacao.Cardinalidade)
                    {
                        case CardinalidadeRelacao.UmParaUm :

                            instanciaAtributo = Activator.CreateInstance(atributo.PropertyType, true);

                            colunaChaveEstrangeira = entidadeCarregada.GetType().GetProperty(configRelacao.AtributoChaveEstrangeira);

                            if (int.Parse(colunaChaveEstrangeira.GetValue(entidadeCarregada, null).ToString()) > 0)
                            {
                                var colunaChaveAtributo = obterColunaChave(instanciaAtributo, false);

                                var anotacaoChaveAtributo = colunaChaveAtributo.GetCustomAttributes(true)
                                                            .FirstOrDefault(ca => ca.GetType().Name.Equals("ColunaDados")) as ColunaDados;

                                colunaChaveAtributo.SetValue(instanciaAtributo, colunaChaveEstrangeira.GetValue(entidadeCarregada, null), null);

                                instanciaAtributo = Consultar(instanciaAtributo, instanciaAtributo.GetType(), null, false);
                            }

                            break;
                        case CardinalidadeRelacao.UmParaMuitos :

                            instanciaAtributo = Activator.CreateInstance(atributo.PropertyType.GetGenericArguments()[0], true);

                            colunaChaveEstrangeira = instanciaAtributo.GetType().GetProperty(configRelacao.AtributoChaveEstrangeira);
                            colunaChaveEstrangeira.SetValue(instanciaAtributo, int.Parse(colunaChavePrimaria.GetValue(entidadeCarregada, null).ToString()), null);

                            instanciaAtributo = Listar(instanciaAtributo, instanciaAtributo.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, false);

                            break;
                        case CardinalidadeRelacao.MuitosParaMuitos :

                            instanciaAtributo = Activator.CreateInstance(atributo.PropertyType, true);

                            if (instanciaAtributo != null)
                            {
                                if (int.Parse(colunaChavePrimaria.GetValue(entidadeCarregada, null).ToString()) > 0)
                                {
                                    if (!instanciaAtributo.GetType().Name.Contains("List"))
                                    {
                                        definirChaveHashEntidade(entidadeCarregada, instanciaAtributo);
                                        replicarHashFilhos(entidadeCarregada, instanciaAtributo);
                                    }
                                    else
                                    {
                                        var instanciaItemFilho = Activator.CreateInstance(atributo.PropertyType.GetGenericArguments()[0], true);

                                        definirChaveHashEntidade(entidadeCarregada, instanciaItemFilho);
                                        replicarHashFilhos(entidadeCarregada, instanciaItemFilho);

                                        instanciaAtributo = Listar(instanciaItemFilho, instanciaItemFilho.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, true, true);
                                    }
                                }
                                else
                                    instanciaAtributo = Listar(instanciaAtributo, instanciaAtributo.GetType(), null, 0, string.Empty, string.Empty, string.Empty, false, false, false, false, true);
                            }
                            break;
                    }
                }

                if ((instanciaAtributo != null) && (instanciaAtributo.GetType().Name.Equals(atributo.PropertyType.Name)))
                    if (!atributo.PropertyType.Name.Contains("List"))
                        atributo.SetValue(entidadeCarregada, instanciaAtributo, null);
                    else
                        atributo.SetValue(entidadeCarregada, (IList)instanciaAtributo, null);
            }
        }

        private object formatarValorEntradaCampoSQL(object valorCampoSQL, int acao, IDataColumn configCampo, Dictionary<object, object> parametrosComando)
        {
            if (valorCampoSQL != null)
            {
                switch (valorCampoSQL.GetType().ToString())
                {
                    case TiposDeDados.InteiroCurto:
                        if (((short)valorCampoSQL == 0) && (!configCampo.EhRequerida()))
                            valorCampoSQL = ValoresPadraoCamposSql.Nulo;
                        break;
                    case TiposDeDados.Inteiro:
                        if (((int)valorCampoSQL == 0) && ((!configCampo.EhRequerida()) || (acao == (int)AcaoPersistencia.Listagem)))
                            valorCampoSQL = ValoresPadraoCamposSql.Nulo;
                        break;
                    case TiposDeDados.InteiroLongo:
                        if (((long)valorCampoSQL == 0) && (!configCampo.EhRequerida()))
                            valorCampoSQL = ValoresPadraoCamposSql.Nulo;
                        break;
                    case TiposDeDados.Texto:
                        valorCampoSQL = "'" + valorCampoSQL + "'";
                        break;
                    case TiposDeDados.DataHora:
                        if (!(((DateTime)valorCampoSQL).Equals(DateTime.MinValue)))
                            valorCampoSQL = "'" + ((DateTime)valorCampoSQL).ToString(FormatoDatas.DataHoraCompleta) + "'";
                        else
                            valorCampoSQL = ValoresPadraoCamposSql.Nulo;
                        break;
                    case TiposDeDados.Binario :
                        parametrosComando.Add(string.Concat("@", configCampo.ObterNomeColuna()),
                                              (byte[])valorCampoSQL);

                        valorCampoSQL = string.Concat("@", configCampo.ObterNomeColuna());
                        
                        break;
                }
            }
            else
            {
                valorCampoSQL = (acao == (int)AcaoPersistencia.Inclusao) ? ValoresPadraoCamposSql.Nulo : null;
            }

            return valorCampoSQL;
        }

        private object formatarValorSaidaCampoSQL(string valorCampoSQL, Type tipoDadoCampo)
        {
            object retorno = null;

            if (!string.IsNullOrEmpty(valorCampoSQL))
                retorno = Convert.ChangeType(valorCampoSQL, tipoDadoCampo);

            return retorno;
        }

        private void validateListableAttributes(Type tipoEntidade, string atributosExibir, out string[] atributosExibicao)
        {
            IEnumerable<PropertyInfo> atributosListaveis = null;
            bool atributoNaoListavel = false;
            string siglaIdioma = ConfigurationManager.AppSettings["Idioma"];

            atributosListaveis = tipoEntidade.GetProperties().
                                              Where(prp => ((PropertyInfo)prp).GetCustomAttributes(true).
                                              Where(ca => ((ca is ColunaDados) || (ca is ColunaRelacional))
                                                       && ((IColunaDados)ca).EhListavel()).Any());

            if (atributosExibir == string.Empty)
            {
                atributosExibicao = new string[atributosListaveis.Count()];

                int cont = 0;
                foreach (var atributoListavel in atributosListaveis)
                {
                    atributosExibicao[cont] = atributoListavel.Name;
                    cont++;
                }
            }
            else
            {
                atributosExibicao = atributosExibir.Split(',');

                foreach (var atributo in atributosExibicao)
                {
                    atributoNaoListavel = !atributosListaveis.Contains(tipoEntidade.GetProperty(atributo));
                    if (!atributoNaoListavel) throw new ExcecaoAtributoNaoListavel(siglaIdioma); break;
                }
            }
        }

        private PropertyInfo getKeyColumn(object entity, bool foreignKey)
        {
            PropertyInfo entityKeyColumn = null;

            if (!foreignKey)
                entityKeyColumn = entity.GetType().GetProperties().FirstOrDefault(fd =>
                                                   (fd.GetCustomAttributes(true).Any(ca =>
                                                   (ca.GetType().Name.Equals("DataColumn")
                                                    && ((ColunaDados)ca).EhChavePrimaria()))));
            else
                colunaChaveEntidade = entidade.GetType().GetProperties().LastOrDefault(fd =>
                                                      (fd.GetCustomAttributes(true).Any(ca =>
                                                      (ca.GetType().Name.Equals("DataColumn")
                                                        && ((ColunaDados)ca).EhChaveEstrangeira()))));

            return colunaChaveEntidade;
        }

        #endregion
    }

    public static class DataType
    {
        #region Declarations

        public const string Short = "System.Int16";
        public const string Integer = "System.Int32";
        public const string Long = "System.Int64";
        public const string String = "System.String";
        public const string DateTime = "System.DateTime";
        public const string Binary = "System.Byte[]";

        #endregion
    }

    public static class SqlDefaultValue
    {
        #region Declarations

        public const string Null = "NULL";
        public const string Zero = "0";
        public const string False = "-1";

        #endregion
    }

    public static class SqlOperator
    {
        #region Declarations

        public const string Equal = " = ";
        public const string Different = " <> ";
        public const string Contains = " LIKE '%{0}%' ";
        public const string And = " AND ";
        public const string Or  = " OR ";
        public const string Major = " > ";
        public const string MajorOrEqual = " >= ";
        public const string Less = " < ";
        public const string LessOrEqual = " <= ";
        public const string IsNull = " IS NULL ";
        public const string In = " IN ({0})";
        public const string Between = "BETWEEN {0} AND {1}";
        public const string Not = " NOT ";

        #endregion
    }
    public static class DateTimeFormat
    {
        #region Declarations

        public const string CompleteDateTime = "yyyy-MM-dd hh:mm:ss";
        public const string NormalDate = "yyyy-MM-dd";
        public const string ShortDate = "yy-MM-dd";

        #endregion
    }

    public enum PersistenceAction
    {
        #region Declarations

        Create = 1,
        Edit = 2,
        Delete = 3,
        List = 4,
        View = 5

        #endregion
    }
}
