using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

namespace WordCount
{
    public class Splitter : ISCPBolt
    {
        private Context ctx;
        /// <summary>
        /// Constructor for the bolt
        /// </summary>
        /// <param name="ctx">Context information provided by the topology</param>
        public Splitter(Context ctx)
        {
            Context.Logger.Info("Splitter constructor called");
            this.ctx = ctx;

            // Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            // Input contains a tuple with a string field (the sentence)
            inputSchema.Add("default", new List<Type>() { typeof(string) });
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            // Outbound contains a tuple with a string field (the word)
            outputSchema.Add("default", new List<Type>() { typeof(string) });
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));
        }

        /// <summary>
        /// Gets a new instance of the bolt
        /// </summary>
        /// <param name="ctx">Context information provided by the topology</param>
        /// <param name="parms">Optional parameters</param>
        /// <returns></returns>
        public static Splitter Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new Splitter(ctx);
        }

        /// <summary>
        /// Process tuples from the data stream
        /// </summary>
        /// <param name="tuple">The tuple to be processed</param>
        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("Execute enter");

            // Get the sentence from the tuple
            string sentence = tuple.GetString(0);
            // Split at space characters
            foreach (string word in sentence.Split(' '))
            {
                Context.Logger.Info("Emit: {0}", word);
                // Emit each word to the data stream
                this.ctx.Emit(new Values(word));
            }

            Context.Logger.Info("Execute exit");
        }
    }
}