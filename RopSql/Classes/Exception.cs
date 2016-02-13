using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Globalization.Language;
using System.Data.RopSql.Resources;

namespace System.Data.RopSql.Exceptions
{
    [Serializable]
    public sealed class ConnectionConfigurationNotFoundException : Exception
    {
        public ConnectionConfigurationNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("ConnectionConfigNotFound"))
        {
        }
    }

    [Serializable]
    public sealed class DataSourceNotFoundException : Exception
    {
        public DataSourceNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("DataSourceNotFound"))
        {
        }
    }

    [Serializable]
    public sealed class InvalidConnectionCredentialsException : Exception
    {
        public InvalidConnectionCredentialsException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("InvalidConnectionCredentials"))
        {
        }
    }

    [Serializable]
    public sealed class SqlInstructionNotFoundException : Exception
    {
        public SqlInstructionNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("SqlInstructionNotFound"))
        {
        }
    }

    [Serializable]
    public sealed class InvalidPersistenceActionException : Exception
    {
        public InvalidPersistenceActionException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("InvalidPersistenceAction"))
        {
        }
    }

    [Serializable]
    public sealed class AttributeNotListableException : Exception
    {
        public AttributeNotListableException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("AtributeNotListable"))
        {
        }
    }

    [Serializable]
    public sealed class RecordNotFoundException : Exception
    {
        public RecordNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("RecordNotFound"))
        {
        }
    }

    [Serializable]
    public sealed class InvalidAttributeException : Exception
    {
        public InvalidAttributeException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("InvalidAttribute"))
        {
        }
    }
}
