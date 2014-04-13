using System;
using System.Collections.Generic;

namespace SGE.Interfaces
{
    public interface IAdaptadorDados
    {
        int Incluir<T>(T entidade);
        int Alterar<T>(T entidade, T entidadeFiltro);
        int Excluir<T>(T entidadeFiltro);
        List<object> Listar<T>(T entidadeFiltro);
        List<object> Listar<T>(T entidadeFiltro, int limiteRegistros);
        List<object> Listar<T>(T entidadeFiltro, string camposAgrupar);
        List<object> Listar<T>(T entidadeFiltro, string camposClassificar, bool ordenacaoDecrescente);
        List<object> Listar<T>(T entidadeFiltro, string camposAgrupar, string camposClassificar, bool ordenacaoDecrescente);
        List<object> Listar<T>(T entidadeFiltro, string camposAgrupar, string camposClassificar, int limiteRegistros, bool ordenacaoDecrescente);
        List<object> Listar<T>(T entidadeFiltro, string camposExibir, string camposAgrupar, string camposClassificar, int limiteRegistros, bool ordenacaoDecrescente);
        object Consultar<T>(T entidadeFiltro);
        void IniciarTransacao();
        void EfetivarTransacao();
        void CancelarTransacao();
    }
}
