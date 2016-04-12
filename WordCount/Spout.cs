using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

namespace WordCount
{
    public class Spout : ISCPSpout
    {
        // Holds the context for an instance of the spout
        private Context ctx;
        // Used to generate random sentences
        private Random r = new Random();
        string[] sentences = new string[]
        {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        };

        /// <summary>
        /// Constructor for the spout
        /// </summary>
        /// <param name="ctx">Context information provided by the topology</param>
        public Spout(Context ctx)
        {
            // Set the instance context
            this.ctx = ctx;

            // Log that the spout started
            Context.Logger.Info("Generator constructor called");

            //Declare the output schema
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            // The schema for the default output stream is
            // a tuple that contains a string field
            outputSchema.Add("default", new List<Type>() { typeof(string) });
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));
        }

        /// <summary>
        /// Gets a new instance of the spout
        /// </summary>
        /// <param name="ctx">Context information provided by the topology</param>
        /// <param name="parms">Optional parameters</param>
        /// <returns></returns>
        public static Spout Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new Spout(ctx);
        }

        /// <summary>
        /// Emits the next tuple to the stream
        /// </summary>
        /// <param name="parms">optional parameters</param>
        public void NextTuple(Dictionary<string, Object> parms)
        {
            Context.Logger.Info("NextTuple enter");
            // The sentence to be emitted
            string sentence;
            // Get a random sentence
            sentence = sentences[r.Next(0, sentences.Length - 1)];
            // Emit the sentence
            this.ctx.Emit(new Values(sentence));
            Context.Logger.Info("NextTuple exit");
        }

        /// <summary>
        /// Handles Ack's from downstream components
        /// </summary>
        /// <param name="seqId">The ID of the tuple that is being acknowledged</param>
        /// <param name="parms">Optional parameters</param>
        public void Ack(long seqId, Dictionary<string, Object> parms)
        {
            // Only used for transactional topologies
        }

        /// <summary>
        /// Handles failed tuples from downstream components
        /// </summary>
        /// <param name="seqId">The ID of the tuple that failed processing</param>
        /// <param name="parms">Optional parameters</param>
        public void Fail(long seqId, Dictionary<string, Object> parms)
        {
            // Only used for transactional topologies
        }
    }
}