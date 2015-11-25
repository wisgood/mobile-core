package com.bi.comm.calculate.topn;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class TopNArgs implements CLPInterface
{

	private String[]				topNParams	= null;
	private HashMap<String, Option>	hashmap		= null;
	private AutoHelpParser			parser		= null;

	@Override
	public void init( String jarName ) throws Exception
	{

		parser = new AutoHelpParser();
		hashmap = new HashMap<String, Option>();

		parser.addFunction( "Calculate the top n max (or min)value of the given column" );
		parser.addUsage( "hadoop jar " + jarName + " " );

		CmdLineParser.Option help = parser.addHelp( parser.addBooleanOption( 'h', "help" ), "return the help doc" );
		hashmap.put( "help", help );

		CmdLineParser.Option input = parser.addHelp( parser.addStringOption( 'i', "input" ), "the input file or path" );
		hashmap.put( "input", input );

		CmdLineParser.Option output = parser
				.addHelp( parser.addStringOption( 'o', "output" ), "the hadoop result output path,the path does not in the cluster" );
		hashmap.put( "output", output );

		CmdLineParser.Option idColumns = parser.addHelp( parser.addStringOption( "idcolumns" ), "the given column index,e.g 0,1" );
		hashmap.put( "idColumns", idColumns );

		CmdLineParser.Option topColumn = parser.addHelp( parser.addStringOption( "topcolumn" ), "the given column index,e.g 0,1" );
		hashmap.put( "topColumn", topColumn );

		CmdLineParser.Option topN = parser.addHelp( parser.addStringOption( "topn" ), "the largest number of n " );
		hashmap.put( "topN", topN );

	}

	public void parse( String[] args ) throws Exception
	{

		parser.parse( args );

		if( args.length == 0 || Boolean.TRUE.equals( parser.getOptionValue( hashmap.get( "help" ) ) ) ) { throw new Exception( "No args or --help" ); }

		topNParams = new String[ 5 ];
		topNParams[ 0 ] = ( String )parser.getOptionValue( hashmap.get( "input" ) );
		topNParams[ 1 ] = ( String )parser.getOptionValue( hashmap.get( "output" ) );
		topNParams[ 2 ] = ( String )parser.getOptionValue( hashmap.get( "idColumns" ) );
		topNParams[ 3 ] = ( String )parser.getOptionValue( hashmap.get( "topColumn" ) );
		topNParams[ 4 ] = ( String )parser.getOptionValue( hashmap.get( "topN" ) );
		if( topNParams[ 0 ] == null || topNParams[ 1 ] == null || topNParams[ 2 ] == null || topNParams[ 3 ] == null || topNParams[ 4 ] == null ) { throw new Exception(
				"the argument value is null" ); }
	}

	public String[] getParams()
	{
		return topNParams;
	}

	public HashMap<String, Option> getHashmap()
	{
		return hashmap;
	}

	public AutoHelpParser getParser()
	{
		return parser;
	}

}
