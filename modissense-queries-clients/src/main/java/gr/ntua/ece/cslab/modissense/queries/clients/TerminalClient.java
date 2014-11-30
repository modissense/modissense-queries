package gr.ntua.ece.cslab.modissense.queries.clients;


import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.UserCheckinsQueryArguments;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.ColumnIndexProtocol;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.POIListProtocol;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class TerminalClient {

    private static Options options;
    private static UserCheckinsQueryArguments arguments;
    private static int resultsToShow = Integer.MAX_VALUE;
    private static String tableName;
    private static Class protocolClass;

    private static void configureOptions(String[] args) throws ParseException {
        options = new Options();
        options.addOption("h", "help", false, "prints this message");

        // points
        options.addOption("from", "point-from", true, "upper left point of rectangular (comma separated values) <x0,y0>");
        options.addOption("to", "point-to", true, "lower right point of rectangular (comma separated values) <x1,y1>");

        //keywords list
        options.addOption("k", "keywords", true, "comma separated keyword list");

        //users list
        options.addOption("u", "users", true, "comma separated user id list");

        // 
        options.addOption("r", "random-users", true, "if set the users are randomized and the argument points to the max user id.");
        options.addOption("o", "output", true, "determine how many POIs to be printed in the output");
        options.addOption("t", "table", true, "define which table to use for the query");
        options.addOption("p", "protocol", true, "which coprocessor to use; one of ColumnIndexProtocol, POIListProtocol");
        
        options.addOption("tf", "time-from", true, "start timestamp");
        options.addOption("tt", "time-to", true, "end timestamp");

        arguments = new UserCheckinsQueryArguments();

        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h")) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp(TerminalClient.class.toString(), options);
            System.exit(1);
        }

        // points parsing
        if (cmd.hasOption("from")) {
            String[] values = cmd.getOptionValue("from").split(",");
            arguments.setxFrom(new Double(values[0]));
            arguments.setyFrom(new Double(values[1]));
        } else {
            System.err.println("I need from point!!");
            System.exit(1);
        }

        if (cmd.hasOption("to")) {
            String[] values = cmd.getOptionValue("to").split(",");
            arguments.setxTo(new Double(values[0]));
            arguments.setyTo(new Double(values[1]));
        } else {
            System.err.println("I need to point!!");
            System.exit(1);
        }

        //  keywords parsing
        if (cmd.hasOption("k")) {
            String[] values = cmd.getOptionValue("k").split(",");
            List<String> keyws = new LinkedList<>();
            keyws.addAll(Arrays.asList(values));
            arguments.setKeywords(keyws);
            
        } else {
            System.err.println("I need keywords here!");
            System.exit(1);
        }

        //user parsing
        if (cmd.hasOption("r")) {
            char[] sn = {'F', 'f', 't'};
            if(cmd.hasOption("u")) {
                int count  = new Integer(cmd.getOptionValue("u"));
                int maxId = new Integer(cmd.getOptionValue("r"));
                Random rand = new Random();
                List<UserIdStruct> userIds = new LinkedList<>();
                for(int i=0;i<count;i++) {
//                    userIds.add(Math.abs(rand.nextLong() % maxId) + 1);
                    userIds.add(new UserIdStruct(sn[rand.nextInt(3)], Math.abs(rand.nextLong() % maxId) + 1));
                }
                arguments.setUserIds(userIds);
            } else {
                System.err.println("I need users here!");
                System.exit(1);                
            }
        } else {
            if (cmd.hasOption("u")) {
                String[] values = cmd.getOptionValue("u").split(",");
                List<UserIdStruct> userIds = new LinkedList<>();
                for (String s : values) 
                    userIds.add(new UserIdStruct(s.charAt(0), new Long(s.substring(1))));
                arguments.setUserIds(userIds);
            } else {
                System.err.println("I need users here!");
                System.exit(1);
            }
        }
        
        // table name set
        if(cmd.hasOption("t")) {
            tableName = cmd.getOptionValue("t");
        } else {
            System.err.println("Which table should I use?");
            System.exit(1);
        }
        
        if(cmd.hasOption("p")) {
            String value = cmd.getOptionValue("p");
            switch (value) {
                case "ColumnIndexProtocol":
                    protocolClass = ColumnIndexProtocol.class;
                    break;
                case "POIListProtocol":
                    protocolClass = POIListProtocol.class;
                    break;
                default:
                    System.err.println("Don't know this protocol!");
                    System.exit(1);
            }
        } else {
            protocolClass = ColumnIndexProtocol.class;
        }
        
        if(cmd.hasOption("o")) {
            resultsToShow = new Integer(cmd.getOptionValue("o"));
        }
        
        if(cmd.hasOption("tf")) {
            arguments.setStartTimestamp(new Long(cmd.getOptionValue("tf")));
        }
        if(cmd.hasOption("tt")) {
            arguments.setEndTimestamp(new Long(cmd.getOptionValue("tt")));
        }
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        configureOptions(args);
        
        UserCheckinsQueryClient client = new UserCheckinsQueryClient();
        client.setProtocol(protocolClass);
        client.setArguments(arguments);
        client.openConnection(tableName);
        client.executeQuery();
        client.closeConnection();
        
        for(POI p : client.getResults().getPOIs()) {
            System.out.format("[%d] %s\t(%.2f,%.2f)\t%s\t%.2f\n", p.getId(), p.getName(), p.getX(), p.getY(), p.getKeywords(), p.getScore());
            if(resultsToShow<=0)
                break;
            else
                resultsToShow--;
        }
        
        
        System.out.println("Execution time:\t"+client.getExecutionTime());
    }
}
