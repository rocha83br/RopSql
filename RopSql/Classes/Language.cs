using System;
using System.Collections;
using System.Reflection;
using System.Globalization;
using System.Data.RopSql.Resources;

namespace System.Globalization.Language
{
	public class LanguageTranslator : IDisposable
    {
        #region Declarations

        private string culture;

        #endregion

        #region Public Methods

        public LanguageTranslator(string cultureAcronym)
        {
            culture = cultureAcronym;
        }

        public string TranslateMessage(string repositoryKey)
        {
            return translateMessage(repositoryKey, culture);
        }

        public void Dispose()
        {
            GC.ReRegisterForFinalize(this);
        }

        #endregion   

        #region Helper Methods

        private string translateMessage(string repositoryKey, string cultureAcronym)
        {
            var messageRepository = MessageRepository.ResourceManager.GetResourceSet(new CultureInfo("pt-BR"), true, true);
            string returnMessage = string.Empty;

            foreach (DictionaryEntry msg in repositorioMensagens)
                if (msg.Key.ToString().Contains(string.Concat(chaveRepositorio, siglaIdioma)))
                {
                    returnMessage = msg.Value.ToString();
                    break;
                }

            return returnMessage;
        }

        #endregion
	}

    public class Language
    {
        #region Declarations

        public const string Portuguese = "_BR";
        public const string English = "_US";
        public const string Spanish = "_ES";
        public const string French = "_FR";
        public const string German = "_DE";

        #endregion
    }
}