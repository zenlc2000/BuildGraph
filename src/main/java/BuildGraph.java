/**
 * Created by zen on 3/7/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.*;

//import org.apache.log4j.spi.LoggerFactory;

//import org.apache.log4j.Logger;


public class BuildGraph extends Configured implements Tool
{
    private static final Logger sLogger = LoggerFactory.getLogger(BuildGraph.class);
    private static DecimalFormat df = new DecimalFormat("0.00");
    private static FileOutputStream logfile;
    private static PrintStream logstream;

    static String basic        = "00-basic";
    static String initial      = "01-initial";
    static String initialcmp   = "02-initialcmp";

    private static class BuildGraphMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text>
    {
        private static int K = 0;
        private static int TRIM5 = 0;
        private static int TRIM3 = 0;

        public void configure(JobConf job)
        {
            K = Integer.parseInt(job.get("K"));
            TRIM5 = Integer.parseInt(job.get("TRIM5"));
            TRIM3 = Integer.parseInt(job.get("TRIM3"));
        }

        public void map(LongWritable lineid, Text nodetxt,
                        OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
            String[] fields = nodetxt.toString().split("\t");

            if (fields.length != 2)
            {
                //System.err.println("Warning: invalid input: \"" + nodetxt.toString() + "\"");
                reporter.incrCounter("Contrail", "input_lines_invalid", 1);
                return;
            }

            String tag = fields[0];

            tag.replaceAll(" ", "_");
            tag.replaceAll(":", "_");
            tag.replaceAll("#", "_");
            tag.replaceAll("-", "_");
            tag.replaceAll(".", "_");

            String seq = fields[1].toUpperCase();

            // Hard chop a few bases off of each end of the read
            if (TRIM5 > 0 || TRIM3 > 0)
            {
                // System.err.println("orig: " + seq);
                seq = seq.substring(TRIM5, seq.length() - TRIM5 - TRIM3);
                // System.err.println("trim: " + seq);
            }

            // Automatically trim Ns off the very ends of reads
            int endn = 0;
            while (endn < seq.length() && seq.charAt(seq.length()-1-endn) == 'N') { endn++; }
            if (endn > 0) { seq = seq.substring(0, seq.length()-endn); }

            int startn = 0;
            while (startn < seq.length() && seq.charAt(startn) == 'N') { startn++; }
            if (startn > 0) { seq = seq.substring(startn, seq.length() - startn); }

            // Check for non-dna characters
            if (seq.matches(".*[^ACGT].*"))
            {
                //System.err.println("WARNING: non-DNA characters found in " + tag + ": " + seq);
                reporter.incrCounter("Contrail", "reads_skipped", 1);
                return;
            }

            // check for short reads
            if (seq.length() <= K)
            {
                //System.err.println("WARNING: read " + tag + " is too short: " + seq);
                reporter.incrCounter("Contrail", "reads_short", 1);
                return;
            }

            // Now emit the edges of the de Bruijn Graph

            char ustate = '5';
            char vstate = 'i';

            Set<String> seenmers = new HashSet<String>();

            String chunkstr = "";
            int chunk = 0;

            int end = seq.length() - K;

            for (int i = 0; i < end; i++)
            {
                String u = seq.substring(i,   i+K);
                String v = seq.substring(i+1, i+1+K);

                String f = seq.substring(i, i+1);
                String l = seq.substring(i+K, i+K+1);
                f = Node.rc(f);

                char ud = Node.canonicaldir(u);
                char vd = Node.canonicaldir(v);

                String t  = Character.toString(ud) + vd;
                String tr = Node.flip_link(t);

                String uc0 = Node.canonicalseq(u);
                String vc0 = Node.canonicalseq(v);

                String uc = Node.str2dna(uc0);
                String vc = Node.str2dna(vc0);

                //System.out.println(u + " " + uc0 + " " + ud + " " + uc);
                //System.out.println(v + " " + vc0 + " " + vd + " " + vc);

                if ((i == 0) && (ud == 'r'))  { ustate = '6'; }
                if (i+1 == end) { vstate = '3'; }

                boolean seen = (seenmers.contains(u) || seenmers.contains(v) || u.equals(v));
                seenmers.add(u);

                if (seen)
                {
                    chunk++;
                    chunkstr = "c" + chunk;
                    //#print STDERR "repeat internal to $tag: $uc u$i $chunk\n";
                }

                //System.out.println(uc + "\t" + t + "\t" + l + "\t" + tag + chunkstr + "\t" + ustate);

                output.collect(new Text(uc),
                        new Text(t + "\t" + l + "\t" + tag + chunkstr + "\t" + ustate));

                if (seen)
                {
                    chunk++;
                    chunkstr = "c" + chunk;
                    //#print STDERR "repeat internal to $tag: $vc v$i $chunk\n";
                }

                //print "$vc\t$tr\t$f\t$tag$chunk\t$vstate\n";

                //System.out.println(vc + "\t" + tr + "\t" + f + "\t" + tag + chunkstr + "\t" + vstate);

                output.collect(new Text(vc),
                        new Text(tr + "\t" + f + "\t" + tag + chunkstr + "\t" + vstate));

                ustate = 'm';
            }

            reporter.incrCounter("Contrail", "reads_good", 1);
            reporter.incrCounter("Contrail", "reads_goodbp", seq.length());
        }
    }

    private static class BuildGraphReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text>
    {
        private static int K = 0;
        private static int MAXTHREADREADS = 0;
        private static int MAXR5 = 0;
        private static boolean RECORD_ALL_THREADS = false;

        public void configure(JobConf job) {
            K = Integer.parseInt(job.get("K"));
            MAXTHREADREADS = Integer.parseInt(job.get("MAXTHREADREADS"));
            MAXR5 = Integer.parseInt(job.get("MAXR5"));
            RECORD_ALL_THREADS = Integer.parseInt(job.get("RECORD_ALL_THREADS")) == 1;
        }

        public void reduce(Text curnode, Iterator<Text> iter,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
            Node node = new Node();

            String mertag = null;
            float cov = 0;

            // Hash keyed based on "f" or "r" direction of the edge
            // Contains nested hash that is keyed based on single-base extension of the k-mer (neighboring k-mer)
            // and stacks upon a list the read ids that contain that neighboring k-mer

            //  storage the de Bruijn in the hash below
            //  {forward / reverse} -> [ { A / C / G / T } -> [read_id1, read_id2, ...]  ]
            Map<String, Map<String, List<String>>> edges = new HashMap<String, Map<String, List<String>>>();

            while(iter.hasNext())
            {
                String valstr = iter.next().toString();
                String [] vals = valstr.split("\t");

                String type     = vals[0]; // edge type between mers
                String neighbor = vals[1]; // id of neighboring node
                String tag      = vals[2]; // id of read contributing to edge
                String state    = vals[3]; // internal or end mer

                // Add the edge to the neighbor
                Map<String, List<String>> neighborinfo = null;
                if (edges.containsKey(type))
                {
                    neighborinfo = edges.get(type);
                }
                else
                {
                    neighborinfo = new HashMap<String, List<String>>();
                    edges.put(type, neighborinfo);
                }


                // Now record the read supports the edge
                List<String> tags = null;
                if (neighborinfo.containsKey(neighbor))
                {
                    tags = neighborinfo.get(neighbor);
                }
                else
                {
                    tags = new ArrayList<String>();
                    neighborinfo.put(neighbor, tags);
                }

                if (tags.size() < MAXTHREADREADS)
                {
                    tags.add(tag);
                }

                // Check on the mertag
                if (mertag == null || (tag.compareTo(mertag) < 0))
                {
                    mertag = tag;
                }

                // Update coverage, offsets
                if (!state.equals("i"))
                {
                    cov++;

                    if (state.equals("6"))
                    {
                        node.addR5(tag, K-1, 1, MAXR5);
                    }
                    else if (state.equals("5"))
                    {
                        node.addR5(tag, 0, 0, MAXR5);
                    }
                }
            }

            node.setMertag(mertag);
            node.setCoverage(cov);

            String seq = Node.dna2str(curnode.toString());
            String rc  = Node.rc(seq);

            node.setstr_raw(curnode.toString());

            seq = seq.substring(1);
            rc  = rc.substring(1);

            char [] dirs = {'f', 'r'};

            for (int d = 0; d < 2; d++)
            {
                String x = Character.toString(dirs[d]);

                int degree = 0;

                for (int e = 0; e < 2; e++)
                {
                    String t = x + dirs[e];

                    if (edges.containsKey(t))
                    {
                        degree += edges.get(t).size();
                    }
                }

                for(int e = 0; e < 2; e++)
                {
                    String t = x + dirs[e];

                    if (edges.containsKey(t))
                    {
                        Map<String, List<String>> edgeinfo = edges.get(t);

                        for (String vc : edgeinfo.keySet())
                        {
                            String v = seq;
                            if (dirs[d] == 'r') { v = rc; }

                            v = v +  vc;

                            if (dirs[e] == 'r') { v = Node.rc(v); }

                            String link = Node.str2dna(v);

                            node.addEdge(t, link);

                            if ((degree > 1) || RECORD_ALL_THREADS)
                            {
                                for (String r : edgeinfo.get(vc))
                                {
                                    node.addThread(t, link, r);
                                }
                            }
                        }
                    }
                }
            }

            output.collect(curnode, new Text(node.toNodeMsg()));
            reporter.incrCounter("Contrail", "nodecount", 1);
        }
    }
    long GLOBALNUMSTEPS = 0;
    long JOBSTARTTIME = 0;

    public void start(String desc)
    {
        msg(desc + ":\t");
        JOBSTARTTIME = System.currentTimeMillis();
        GLOBALNUMSTEPS++;
    }

    public static void msg(String msg)
    {
        logstream.print(msg);
        System.out.print(msg);
    }

    public void end(RunningJob job) throws IOException
    {
        long endtime = System.currentTimeMillis();
        long diff = (endtime - JOBSTARTTIME) / 1000;

        msg(job.getJobID() + " " + diff + " s");

        if (!job.isSuccessful())
        {
            System.out.println("Job was not successful");
            System.exit(1);
        }
    }
    ///////////////////////////////////////////////////////////////////////////

    public void buildInitial(String inputPath, String basePath, String basic, String initial, String initialcmp) throws Exception
    {
        RunningJob job;

            start("Build Initial");
            BuildGraph bg = new BuildGraph();
            job = bg.run(inputPath, basePath + basic);
            end(job);

            long nodecnt      = counter(job, "nodecount");
            long reads_goodbp = counter(job, "reads_goodbp");
            long reads_good   = counter(job, "reads_good");
            long reads_short  = counter(job, "reads_short");
            long reads_skip   = counter(job, "reads_skipped");

            long reads_all = reads_good + reads_short + reads_skip;

            if (reads_good == 0)
            {
                throw new IOException("No good reads");
            }

            String frac_reads = df.format(100*reads_good/reads_all);
            msg("  " + nodecnt + " nodes [" + reads_good +" (" + frac_reads + "%) good reads, " + reads_goodbp + " bp]\n");
    }

    public long counter(RunningJob job, String tag) throws IOException
    {
        return job.getCounters().findCounter("Contrail", tag).getValue();
    }


    public RunningJob run(String inputPath, String outputPath) throws Exception
    {
        sLogger.info("Tool name: BuildGraph");
        sLogger.info(" - input: "  + inputPath);
        sLogger.info(" - output: " + outputPath);

        JobConf conf = new JobConf(Stats.class);
        conf.setJobName("BuildGraph " + inputPath + " " + ContrailConfig.K);

        ContrailConfig.initializeConfiguration(conf);

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(BuildGraphMapper.class);
        conf.setReducerClass(BuildGraphReducer.class);

        //delete the output directory if it exists already
//        FileSystem.get(conf).delete(new Path(outputPath), true);

        return JobClient.runJob(conf);
    }

    public int run(String[] args) throws Exception
    {
        String dataset = "arbrcrd";
        if (dataset.equals("arbrcrd"))
        {
            ContrailConfig.hadoopReadPath = "/Users/zen/Dropbox/git/BuildGraph/data/arbrcrd.36.sfa";
            ContrailConfig.hadoopBasePath =  "/Users/zen/Dropbox/git/BuildGraph/output/";
            ContrailConfig.localBasePath = ContrailConfig.hadoopBasePath + "work";

            //ContrailConfig.RESTART_SCAFF_STEP = "update";
            //ContrailConfig.RESTART_SCAFF_FRONTIER = 3;
            //ContrailConfig.STARTSTAGE = "scaffolding";
            //ContrailConfig.STOPSTAGE = "buildInitial";

            ContrailConfig.K = 25;
            ContrailConfig.LOW_COV_THRESH = 5.0f;
            ContrailConfig.MAX_LOW_COV_LEN = 50;
            ContrailConfig.MIN_THREAD_WEIGHT = 5;

            ContrailConfig.INSERT_LEN = 100;
            ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
            ContrailConfig.MIN_UNIQUE_COV = 10.0f;
            ContrailConfig.MAX_UNIQUE_COV = 30.0f;
        }
       else
        {
            ContrailConfig.parseOptions(args);
        }

        long starttime = System.currentTimeMillis();
        logfile = new FileOutputStream(ContrailConfig.localBasePath+"contrail.log", true);
        logstream = new PrintStream(logfile);

        String inputPath = ContrailConfig.hadoopReadPath;
        String outputPath = ContrailConfig.hadoopBasePath;

        buildInitial(ContrailConfig.hadoopReadPath, ContrailConfig.hadoopBasePath, basic, initial, initialcmp);
        run(inputPath, outputPath);

        long endtime = System.currentTimeMillis();

        float diff = (float) (((float) (endtime - starttime)) / 1000.0);

        System.out.println("Runtime: " + diff + " s");

        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new BuildGraph(), args);
        System.exit(res);
    }
}
