package weka.core.converters;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.OptionHandler;
import weka.core.Range;
import weka.core.Utils;
import weka.core.converters.IncrementalConverter;
import weka.core.converters.StreamTokenizerUtils;
import weka.core.converters.ArffLoader.ArffReader;
import weka.core.converters.CSVLoader.TYPE;

public class InstanceMaker implements java.io.Serializable{

	  private static final long serialVersionUID1 = 1L;
	
	  /** Holds the determined structure (header) of the data set. */
	  public transient Instances m_structure = null;

	  /** Holds the source of the data set. */
	  protected File m_sourceFile = null;
	
	  /** For serialization */
	  private static final long serialVersionUID = -1300595850715808438L;

	  /** the file extension. */
	  public static String FILE_EXTENSION = ".csv";

	  /** The reader for the data. */
	  protected transient BufferedReader m_sourceReader;

	  /** Tokenizer for the data. */
	  protected transient StreamTokenizer m_st;

	  protected transient File m_tempFile;
	  protected transient PrintWriter m_dataDumper;

	  /** the field separator. */
	  protected String m_FieldSeparator = ",";

	  /** The placeholder for missing values. */
	  protected String m_MissingValue = "?";

	  /** The range of attributes to force to type nominal. */
	  protected Range m_NominalAttributes = new Range();

	  /** The user-supplied legal nominal values - each entry in the list is a spec */
	  protected List<String> m_nominalLabelSpecs = new ArrayList<String>();

	  /** The range of attributes to force to type string. */
	  protected Range m_StringAttributes = new Range();

	  /** The range of attributes to force to type date */
	  protected Range m_dateAttributes = new Range();

	  /** The range of attributes to force to type numeric */
	  protected Range m_numericAttributes = new Range();

	  /** The formatting string to use to parse dates */
	  protected String m_dateFormat = "yyyy-MM-dd'T'HH:mm:ss";

	  /** The formatter to use on dates */
	  protected SimpleDateFormat m_formatter;

	  /** whether the csv file contains a header row with att names */
	  protected boolean m_noHeaderRow = false;

	  /** enclosure character(s) to use for strings */
	  protected String m_Enclosures = "\",\'";

	  /** The in memory row buffer */
	  protected List<String> m_rowBuffer;

	  /** The maximum number of rows to hold in memory at any one time */
	  protected int m_bufferSize = 100;

	  /** Lookup for nominal values */
	  protected Map<Integer, LinkedHashSet<String>> m_nominalVals;

	  /** Reader used to process and output data incrementally */
	  protected ArffReader m_incrementalReader;

	  protected transient int m_rowCount;

	  /**
	   * Array holding field separator and enclosures to pass through to the
	   * underlying ArffReader
	   */
	  protected String[] m_fieldSeparatorAndEnclosures;
	  protected ArrayList<Object> m_current;
	  protected TYPE[] m_types;
	  private int m_numBufferedRows;
	  
	  public void setOptions(String[] options) throws Exception {
		    String tmpStr;

		    setNoHeaderRowPresent(Utils.getFlag('H', options));
	  }
	  /**
	   * Set whether there is no header row in the data.
	   *
	   * @param b true if there is no header row in the data
	   */
	  public void setNoHeaderRowPresent(boolean b) {
	    m_noHeaderRow = b;
	  }

	
	public void readHeader(String firstLine) throws IOException {
	    m_rowCount = 1;
	    m_incrementalReader = null;
	    m_current = new ArrayList<Object>();

	    m_rowBuffer = new ArrayList<String>();

	    String firstRow = firstLine;
	    if (firstRow == null) {
	      throw new IOException("No data in the file!");
	    }
	    if (m_noHeaderRow) {
	      m_rowBuffer.add(firstRow);
	    }

	    ArrayList<Attribute> attribNames = new ArrayList<Attribute>();

	    // now tokenize to determine attribute names (or create att names if
	    // no header row
	    StringReader sr = new StringReader(firstRow + "\n");
	    // System.out.print(firstRow + "\n");
	    m_st = new StreamTokenizer(sr);
	    initTokenizer(m_st);

	    m_st.ordinaryChar(m_FieldSeparator.charAt(0));

	    int attNum = 1;
	    StreamTokenizerUtils.getFirstToken(m_st);
	    if (m_st.ttype == StreamTokenizer.TT_EOF) {
	      StreamTokenizerUtils.errms(m_st, "premature end of file");
	    }
	    boolean first = true;
	    boolean wasSep;

	    while (m_st.ttype != StreamTokenizer.TT_EOL
	      && m_st.ttype != StreamTokenizer.TT_EOF) {
	      // Get next token

	      if (!first) {
	        StreamTokenizerUtils.getToken(m_st);
	      }

	      if (m_st.ttype == m_FieldSeparator.charAt(0)
	        || m_st.ttype == StreamTokenizer.TT_EOL) {
	        wasSep = true;
	      } else {
	        wasSep = false;

	        String attName = null;

	        if (m_noHeaderRow) {
	          attName = "att" + attNum;
	          attNum++;
	        } else {
	          attName = m_st.sval;
	        }

	        attribNames.add(new Attribute(attName, (java.util.List<String>) null));
	      }
	      if (!wasSep) {
	        StreamTokenizerUtils.getToken(m_st);
	      }
	      first = false;
	    }
	    String relationName;
	    if (m_sourceFile != null) {
	        relationName =
	          (m_sourceFile.getName()).replaceAll("\\.[cC][sS][vV]$", "");
	      } else {
	        relationName = "stream";
	      }
	    
	    m_structure = new Instances(relationName, attribNames, 0);
	    m_NominalAttributes.setUpper(m_structure.numAttributes() - 1);
	    m_StringAttributes.setUpper(m_structure.numAttributes() - 1);
	    m_dateAttributes.setUpper(m_structure.numAttributes() - 1);
	    m_numericAttributes.setUpper(m_structure.numAttributes() - 1);
	    m_nominalVals = new HashMap<Integer, LinkedHashSet<String>>();

	    m_types = new TYPE[m_structure.numAttributes()];
	    for (int i = 0; i < m_structure.numAttributes(); i++) {
	      if (m_NominalAttributes.isInRange(i)) {
	        m_types[i] = TYPE.NOMINAL;
	        LinkedHashSet<String> ts = new LinkedHashSet<String>();
	        m_nominalVals.put(i, ts);
	      } else if (m_StringAttributes.isInRange(i)) {
	        m_types[i] = TYPE.STRING;
	      } else if (m_dateAttributes.isInRange(i)) {
	        m_types[i] = TYPE.DATE;
	      } else if (m_numericAttributes.isInRange(i)) {
	        m_types[i] = TYPE.NUMERIC;
	      } else {
	        m_types[i] = TYPE.UNDETERMINED;
	      }
	    }

	    if (m_nominalLabelSpecs.size() > 0) {
	      for (String spec : m_nominalLabelSpecs) {
	        String[] attsAndLabels = spec.split(":");
	        if (attsAndLabels.length == 2) {
	          String[] labels = attsAndLabels[1].split(",");
	          try {
	            // try as a range string first
	            Range tempR = new Range();
	            tempR.setRanges(attsAndLabels[0].trim());
	            tempR.setUpper(m_structure.numAttributes() - 1);

	            int[] rangeIndexes = tempR.getSelection();
	            for (int i = 0; i < rangeIndexes.length; i++) {
	              m_types[rangeIndexes[i]] = TYPE.NOMINAL;
	              LinkedHashSet<String> ts = new LinkedHashSet<String>();
	              for (String lab : labels) {
	                ts.add(lab);
	              }
	              m_nominalVals.put(rangeIndexes[i], ts);
	            }
	          } catch (IllegalArgumentException e) {
	            // one or more named attributes?
	            String[] attNames = attsAndLabels[0].split(",");
	            for (String attN : attNames) {
	              Attribute a = m_structure.attribute(attN.trim());
	              if (a != null) {
	                int attIndex = a.index();
	                m_types[attIndex] = TYPE.NOMINAL;
	                LinkedHashSet<String> ts = new LinkedHashSet<String>();
	                for (String lab : labels) {
	                  ts.add(lab);
	                }
	                m_nominalVals.put(attIndex, ts);
	              }
	            }
	          }
	        }
	      }
	    }

	    // Prevents the first row from getting lost in the
	    // case where there is no header row and we're
	    // running in batch mode
//	    if (m_noHeaderRow && getRetrieval() == BATCH) {
//	      StreamTokenizer tempT = new StreamTokenizer(new StringReader(firstRow));
//	      initTokenizer(tempT);
//	      tempT.ordinaryChar(m_FieldSeparator.charAt(0));
//	      String checked = getInstance(tempT);
//	      dumpRow(checked);
//	    }

//	    m_st = new StreamTokenizer(m_sourceReader);
//	    initTokenizer(m_st);
//	    m_st.ordinaryChar(m_FieldSeparator.charAt(0));

	    // try and determine a more accurate structure from the first batch
//	    readData(false);
	    makeStructure();
	  }
	
	 protected void makeStructure() {
	    // make final structure
	    ArrayList<Attribute> attribs = new ArrayList<Attribute>();
	    for (int i = 0; i < m_types.length; i++) {
	      if (m_types[i] == TYPE.STRING || m_types[i] == TYPE.UNDETERMINED) {
	        attribs.add(new Attribute(m_structure.attribute(i).name(),
	          (java.util.List<String>) null));
	      } else if (m_types[i] == TYPE.NUMERIC) {
	        attribs.add(new Attribute(m_structure.attribute(i).name()));
	      } else if (m_types[i] == TYPE.NOMINAL) {
	        LinkedHashSet<String> vals = m_nominalVals.get(i);
	        ArrayList<String> theVals = new ArrayList<String>();
	        if (vals.size() > 0) {
	          for (String v : vals) {
	            /*
	             * if (v.startsWith("'") || v.startsWith("\"")) { v = v.substring(1,
	             * v.length() - 1); }
	             */
	            theVals.add(v);
	          }
	        } else {
	          theVals.add("*unknown*");
	        }
	        attribs.add(new Attribute(m_structure.attribute(i).name(), theVals));
	      } else {
	        attribs
	          .add(new Attribute(m_structure.attribute(i).name(), m_dateFormat));
	      }
	    }
	    m_structure = new Instances(m_structure.relationName(), attribs, 0);
	  }
	
	private void initTokenizer(StreamTokenizer tokenizer) {
	    tokenizer.resetSyntax();
	    tokenizer.whitespaceChars(0, (' ' - 1));
	    tokenizer.wordChars(' ', '\u00FF');
	    tokenizer.whitespaceChars(m_FieldSeparator.charAt(0),
	      m_FieldSeparator.charAt(0));
	    // tokenizer.commentChar('%');

	    String[] parts = m_Enclosures.split(",");
	    for (String e : parts) {
	      if (e.length() > 1 || e.length() == 0) {
	        throw new IllegalArgumentException(
	          "Enclosures can only be single characters");
	      }
	      tokenizer.quoteChar(e.charAt(0));
	    }

	    tokenizer.eolIsSignificant(true);
	  }
	
	private boolean readData(boolean dump) throws IOException {
	    if (m_sourceReader == null) {
	      throw new IOException("No source has been specified");
	    }

//	    boolean finished = false;
//	    do {
//	      String checked = getInstance(m_st);
//	      if (checked == null) {
//	        return false;
//	      }
//
//	      if (dump) {
//	        dumpRow(checked);
//	      }
//	      m_rowBuffer.add(checked);
//
//	      if (m_rowBuffer.size() == m_bufferSize) {
//	        finished = true;
//
//	        if (getRetrieval() == BATCH) {
//	          m_rowBuffer.clear();
//	        }
//	      }
//	    } while (!finished);

	    return true;
	  }
	
	
    /** the tokenizer for reading the stream */
    protected StreamTokenizer m_Tokenizer;

    /** Buffer of values for sparse instance */
    protected double[] m_ValueBuffer;

    /** Buffer of indices for sparse instance */
    protected int[] m_IndicesBuffer;

    protected List<Integer> m_stringAttIndices;

    /** the actual data */
    protected Instances m_Data;

    /** the number of lines read so far */
    protected int m_Lines;

    protected boolean m_batchMode = true;

    /**
     * Whether the values for string attributes will accumulate in the header
     * when reading incrementally
     */
    protected boolean m_retainStringValues = false;

    /** Field separator (single character string) to use instead of the defaults */
    protected String m_fieldSeparator;

    /** List of (single character) enclosures to use instead of the defaults */
    protected List<String> m_enclosures;

	
    /**
     * Reads a single instance using the tokenizer and returns it.
     * 
     * @param flag if method should test for carriage return after each instance
     * @return null if end of file has been reached
     * @throws IOException if the information is not read successfully
     */
    
    public Instance convertToInstance(String s, Instances template){
    	m_Tokenizer = new StreamTokenizer(new StringReader(s));
    	m_Data = template;
    	m_batchMode = true;
    	m_retainStringValues = true;
    	initTokenizer();
    	try {
			getFirstToken();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	Instance i = null;
    	try {
			i = getInstanceFull(false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return i;
    }
    
    protected Instance getInstanceFull(boolean flag) throws IOException {
      double[] instance = new double[m_Data.numAttributes()];
      int index;

      // Get values for all attributes.
      for (int i = 0; i < m_Data.numAttributes(); i++) {
        // Get next token
        if (i > 0) {
          getNextToken();
        }

        if(m_Tokenizer.ttype == StreamTokenizer.TT_EOF)
        	return null;
        
        // Check if value is missing.
        if (m_Tokenizer.ttype == '?') {
          instance[i] = Utils.missingValue();
        } else {

          // Check if token is valid.
          if (m_Tokenizer.ttype != StreamTokenizer.TT_WORD) {
            errorMessage("not a valid value");
          }
          switch (m_Data.attribute(i).type()) {
          case Attribute.NOMINAL:
            // Check if value appears in header.
            index = m_Data.attribute(i).indexOfValue(m_Tokenizer.sval);
            if (index == -1) {
              errorMessage("nominal value not declared in header");
            }
            instance[i] = index;
            break;
          case Attribute.NUMERIC:
            // Check if value is really a number.
            try {
              instance[i] = Double.valueOf(m_Tokenizer.sval).doubleValue();
            } catch (NumberFormatException e) {
              errorMessage("number expected");
            }
            break;
          case Attribute.STRING:
            if (m_batchMode || m_retainStringValues) {
            	 instance[i] =
                m_Data.attribute(i).addStringValue(m_Tokenizer.sval);
            } else {
              instance[i] = 0;
              m_Data.attribute(i).setStringValue(m_Tokenizer.sval);
            }
            break;
          case Attribute.DATE:
            try {
              instance[i] = m_Data.attribute(i).parseDate(m_Tokenizer.sval);
            } catch (ParseException e) {
              errorMessage("unparseable date: " + m_Tokenizer.sval);
            }
            break;
          case Attribute.RELATIONAL:
            try {
              ArffReader arff =
                new ArffReader(new StringReader(m_Tokenizer.sval), m_Data
                  .attribute(i).relation(), 0);
              Instances data = arff.getData();
              instance[i] = m_Data.attribute(i).addRelation(data);
            } catch (Exception e) {
              throw new IOException(e.toString() + " of line " + getLineNo());
            }
            break;
          default:
            errorMessage("unknown attribute type in column " + i);
          }
        }
      }

      double weight = 1.0;
      if (flag) {
        // check for an instance weight
        weight = getInstanceWeight();
        if (!Double.isNaN(weight)) {
          getLastToken(true);
        } else {
          weight = 1.0;
        }
      }

      // Add instance to dataset
      Instance inst = new DenseInstance(weight, instance);
      inst.setDataset(m_Data);

      return inst;
    }
    
    protected void getNextToken() throws IOException {
        if (m_Tokenizer.nextToken() == StreamTokenizer.TT_EOL) {
          errorMessage("premature end of line");
        }
        if (m_Tokenizer.ttype == StreamTokenizer.TT_EOF) {
          errorMessage("premature end of file");
        } else if ((m_Tokenizer.ttype == '\'') || (m_Tokenizer.ttype == '"')) {
          m_Tokenizer.ttype = StreamTokenizer.TT_WORD;
        } else if ((m_Tokenizer.ttype == StreamTokenizer.TT_WORD)
          && (m_Tokenizer.sval.equals("?"))) {
          m_Tokenizer.ttype = '?';
        }
      }
    
    protected void initTokenizer() {
        m_Tokenizer.resetSyntax();
        m_Tokenizer.whitespaceChars(0, ' ');
        m_Tokenizer.wordChars(' ' + 1, '\u00FF');
        if (m_fieldSeparator != null) {
          m_Tokenizer.whitespaceChars(m_fieldSeparator.charAt(0),
            m_fieldSeparator.charAt(0));
        } else {
          m_Tokenizer.whitespaceChars(',', ',');
        }
        m_Tokenizer.commentChar('%');
        if (m_enclosures != null && m_enclosures.size() > 0) {
          for (String e : m_enclosures) {
            m_Tokenizer.quoteChar(e.charAt(0));
          }
        } else {
          m_Tokenizer.quoteChar('"');
          m_Tokenizer.quoteChar('\'');
        }
        m_Tokenizer.ordinaryChar('{');
        m_Tokenizer.ordinaryChar('}');
        m_Tokenizer.eolIsSignificant(true);
      }
    protected void initBuffers() {
        m_ValueBuffer = new double[m_Data.numAttributes()];
        m_IndicesBuffer = new int[m_Data.numAttributes()];

        m_stringAttIndices = new ArrayList<Integer>();
        if (m_Data.checkForStringAttributes()) {
          for (int i = 0; i < m_Data.numAttributes(); i++) {
            if (m_Data.attribute(i).isString()) {
              m_stringAttIndices.add(i);
            }
          }
        }
      }
    
    public int getLineNo() {
        return m_Lines + m_Tokenizer.lineno();
      }
    
    protected void getLastToken(boolean endOfFileOk) throws IOException {
        if ((m_Tokenizer.nextToken() != StreamTokenizer.TT_EOL)
          && ((m_Tokenizer.ttype != StreamTokenizer.TT_EOF) || !endOfFileOk)) {
          errorMessage("end of line expected");
        }
      }
    protected void getFirstToken() throws IOException {
        while (m_Tokenizer.nextToken() == StreamTokenizer.TT_EOL) {
        }
        ;

        if ((m_Tokenizer.ttype == '\'') || (m_Tokenizer.ttype == '"')) {
          m_Tokenizer.ttype = StreamTokenizer.TT_WORD;
        } else if ((m_Tokenizer.ttype == StreamTokenizer.TT_WORD)
          && (m_Tokenizer.sval.equals("?"))) {
          m_Tokenizer.ttype = '?';
        }
      }
    
    protected double getInstanceWeight() throws IOException {
        double weight = Double.NaN;
        m_Tokenizer.nextToken();
        if (m_Tokenizer.ttype == StreamTokenizer.TT_EOL
          || m_Tokenizer.ttype == StreamTokenizer.TT_EOF) {
          return weight;
        }
        // see if we can read an instance weight
        // m_Tokenizer.pushBack();
        if (m_Tokenizer.ttype == '{') {
          m_Tokenizer.nextToken();
          String weightS = m_Tokenizer.sval;
          // try to parse weight as a double
          try {
            weight = Double.parseDouble(weightS);
          } catch (NumberFormatException e) {
            // quietly ignore
            return weight;
          }
          // see if we have the closing brace
          m_Tokenizer.nextToken();
          if (m_Tokenizer.ttype != '}') {
            errorMessage("Problem reading instance weight");
          }
        }
        return weight;
      }
    
    protected void errorMessage(String msg) throws IOException {
        String str = msg + ", read " + m_Tokenizer.toString();
        if (m_Lines > 0) {
          int line = Integer.parseInt(str.replaceAll(".* line ", ""));
          str = str.replaceAll(" line .*", " line " + (m_Lines + line - 1));
        }
        throw new IOException(str);
      }


}