using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

namespace WordCount
{
    [Active(true)]
    class Program : TopologyDescriptor
    {
        static void Main(string[] args)
        {
        }

        /// <summary>
        /// Get a topology builder for this topology
        /// </summary>
        /// <returns></returns>
        public ITopologyBuilder GetTopologyBuilder()
        {
            // Create a new topology, with a name of "WordCount" and the current date
            TopologyBuilder topologyBuilder = new TopologyBuilder("WordCount" + DateTime.Now.ToString("yyyyMMddHHmmss"));

            // Add the spout to the topology.
            // Name the component 'sentences'
            // Name the field that is emitted as 'sentence'
            topologyBuilder.SetSpout(
                "sentences",
                Spout.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"sentence"}}
                },
                1);
            // Add the splitter bolt.
            // Name the component 'splitter'
            // Name the field that is emitted 'word'
            // Use shuffleGrouping to distribute incoming tuples
            //   from the 'sentences' spout across instances of
            //   the splitter
            topologyBuilder.SetBolt(
                "splitter",
                Splitter.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>() {"word" } }
                },
                1).shuffleGrouping("sentences");
            // Add the counter bolt.
            // Name the component 'counter'.
            // Name the fields that are emitted 'word' and 'count'
            // Use fieldsGrouping to ensure that tuples are routed
            //   to counter instances based on the contents of field
            //   position 0 ('word'). This could also be List<string>(){"word"}
            //   Instead of position.
            //   This ensures that words go to the same instance. For example, 'jumped'
            //   will always go to the same instance, while 'cow' might always go to another instance.
            topologyBuilder.SetBolt(
                "counter",
                Counter.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"word", "count"}}
                },
                1).fieldsGrouping("splitter", new List<int>() { 0 });

            // Return the builder
            return topologyBuilder;
        }
    }
}

