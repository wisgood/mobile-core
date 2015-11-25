package com.bi.comm.calculate.frequency;

import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.Option;

import java.util.HashMap;

import com.bi.comm.util.AutoHelpParser;
import com.bi.comm.util.CLPInterface;

public class FrequencyArgs implements CLPInterface
{

	private String[]				frequencyParams	= null;
	private HashMap<String, Option>	hashmap		= null;
	private AutoHelpParser			parser		= null;

	@Override
	public void init( String jarName ) throws Exception
	{

		parser = new AutoHelpParser();
		hashmap = new HashMap<String, Option>();

		parser.addFunction( "compute the frequency of the given column" );
		parser.addUsage( "hadoop jar frequencybycol.jar " );

		CmdLineParser.Option help = parser.addHelp( parser.addBooleanOption( 'h', "help" ), "return the help doc" );
		hashmap.put( "help", help );

		CmdLineParser.Option input = parser.addHelp( parser.addStringOption( 'i', "input" ), "the input file or path" );
		hashmap.put( "input", input );

		CmdLineParser.Option output = parser
				.addHelp( parser.addStringOption( 'o', "output" ), "the hadoop result output path,the path does not in the cluster" );
		hashmap.put( "output", output );

		CmdLineParser.Option idColumns = parser.addHelp( parser.addStringOption( "idcolumns" ), "the given column index,e.g 0,1" );
		hashmap.put( "idcolumns", idColumns );

		CmdLineParser.Option frequencyColumn = parser.addHelp( parser.addStringOption( "frequencycolumn" ), "the column index of frequent,e.g 0" );
		hashmap.put( "frequencycolumn", frequencyColumn );

	}

	public void parse( String[] args ) throws Exception
	{

		parser.parse( args );

		if( args.length == 0 || Boolean.TRUE.equals( parser.getOptionValue( hashmap.get( "help" ) ) ) ) { throw new Exception( "No args or --help" ); }

		frequencyParams = new String[ 4];
		frequencyParams[ 0 ] = ( String )parser.getOptionValue( hashmap.get( "input" ) );
		frequencyParams[ 1 ] = ( String )parser.getOptionValue( hashmap.get( "output" ) );
		frequencyParams[ 2 ] = ( String )parser.getOptionValue( hashmap.get( "idcolumns" ) );
		frequencyParams[ 3 ] = ( String )parser.getOptionValue( hashmap.get( "frequencycolumn" ) );
		if( frequencyParams[ 0 ] == null || frequencyParams[ 1 ] == null || frequencyParams[ 2 ] == null || frequencyParams[ 3 ] == null ) { throw new Exception(
				"the argument value is null" ); }
	}

	public String[] getParams()
	{
		return frequencyParams;
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
