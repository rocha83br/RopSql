using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace System.Data.RopSql
{
    public static class Parallelizer
    {
        public static void StartNewProcess(ParameterizedThreadStart startMethod, ParallelParam argument)
        {
            new Thread(startMethod).Start(argument);
        }
    }

    public class ParallelParam
    {
        public object Param1 = null;
        public object Param2 = null;
        public object Param3 = null;
        public object Param4 = null;
        public object Param5 = null;
        public object Param6 = null;
    }
}
