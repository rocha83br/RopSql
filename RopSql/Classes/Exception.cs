using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Globalization.Language;
using System.Data.RopSql.Resources;

namespace System.Data.RopSql.Exceptions
{
    public sealed class ConnectionConfigurationNotFoundException : Exception
    {
        public ConnectionConfigurationNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("ConnectionConfigNotFound"))
        {
        }
    }

    public sealed class DataSourceNotFoundException : Exception
    {
        public DataSourceNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("DataSourceNotFound"))
        {
        }
    }

    public sealed class InvalidConnectionCredentialsException : Exception
    {
        public InvalidConnectionCredentialsException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("InvalidConnectionCredentials"))
        {
        }
    }

    public sealed class SqlInstructionNotFoundException : Exception
    {
        public SqlInstructionNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("SqlInstructionNotFound"))
        {
        }
    }

    public sealed class InvalidPersistenceActionException : Exception
    {
        public InvalidPersistenceActionException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("InvalidPersistenceAction"))
        {
        }
    }

    public sealed class AttributeNotListableException : Exception
    {
        public AttributeNotListableException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("AtributeNotListable"))
        {
        }
    }

    public sealed class RecordNotFoundException : Exception
    {
        public RecordNotFoundException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("RecordNotFound"))
        {
        }
    }

    public sealed class InvalidAttributeException : Exception
    {
        public InvalidAttributeException(string cultureAcronym)
            : base(new LanguageTranslator(cultureAcronym).TranslateMessage("InvalidAttribute"))
        {
        }
    }
}
