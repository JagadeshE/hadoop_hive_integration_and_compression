import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class hadoop_hive_compression {
	private static final Log LOG = LogFactory.getLog(hadoop_hive_compression.class);

	static void findDifference(String beforedate, String Afterdate) {

		// SimpleDateFormat converts the
		// string format to date object
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");

		// Try Block
		try {

			// parse method is used to parse
			// the text from a string to
			// produce the date
			Date d1 = sdf.parse(beforedate);
			Date d2 = sdf.parse(Afterdate);

			// Calucalte time difference
			// in milliseconds
			long difference_In_Time = d2.getTime() - d1.getTime();

			// Calucalte time difference in
			// seconds, minutes, hours, years,
			// and days

			long difference_In_milliSeconds = difference_In_Time % 1000;

			long difference_In_Seconds = (difference_In_Time / 1000) % 60;

			long difference_In_Minutes = (difference_In_Time / (1000 * 60)) % 60;

			long difference_In_Hours = (difference_In_Time / (1000 * 60 * 60)) % 24;

			long difference_In_Years = (difference_In_Time / (1000l * 60 * 60 * 24 * 365));

			long difference_In_Days = (difference_In_Time / (1000 * 60 * 60 * 24)) % 365;

			// Print the date difference in
			// years, in days, in hours, in
			// minutes, and in seconds

			System.out.print("Compression time is: ");

			System.out.println(
//                difference_In_Years + " years, "
//                + difference_In_Days + " days, " 
//                +
					difference_In_Hours + " hours, " + difference_In_Minutes + " minutes, " + difference_In_Seconds
							+ " seconds, " + difference_In_milliSeconds + " milliseconds");
		}

		// Catch the Exception
		catch (ParseException e) {
			e.printStackTrace();
		}
	}

	// HIVE DRIVER
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		InputStream in = null;
		OutputStream out = null;

		// DATE STRING
		String beforedate = null;
		String Afterdate = null;

		try {
			FileSystem fs = FileSystem.get(conf);

			Scanner input_sc = new Scanner(System.in); // System.in is a standard input stream.
			System.out.print("Enter a Compression File Path with file name: ");
			String intput_file_path = input_sc.nextLine(); // reads string.

			File f = new File(intput_file_path);
	        long fileSize = f.length();
			
			// Input file - local file system
//		   in = new BufferedInputStream(new FileInputStream("/home/jagadesh/hadoop_data/log.txt"));
			in = new BufferedInputStream(new FileInputStream(intput_file_path));

			// Output file path in HDFS

			Scanner output_sc = new Scanner(System.in); // System.in is a standard input stream.
			System.out.print("Enter a Compression File Saved Path with file name in HDFS: ");
			String saved_file_path = output_sc.nextLine(); // reads string.

//		   Path outFile = new Path("/home/hadoop/hadoop/files/Zstd_out/test.zst");
			Path outFile = new Path(saved_file_path);

			// Verifying if the output file already exists
			if (fs.exists(outFile)) {
				throw new IOException("Output file already exists");
			}
			out = fs.create(outFile);

			// Zstd comression
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.ZStandardCodec");

			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date now = new Date();
			beforedate = sdfDate.format(now);

			CompressionOutputStream compressionOutputStream = codec.createOutputStream(out);

			try {
				IOUtils.copyBytes(in, compressionOutputStream, 4096, false);
				compressionOutputStream.finish();

			} finally {
				IOUtils.closeStream(in);
				IOUtils.closeStream(compressionOutputStream);
				LOG.info("Compression FILE SAVED IN HDFS" + ".");
				System.out.println("Compression Finsihed" + "...");

				SimpleDateFormat sdfDate2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				Date now2 = new Date();
				Afterdate = sdfDate2.format(now2);
				findDifference(beforedate, Afterdate);

                // PRINT FILE SIZE OF BEFORE AND AFTER COMPRESSION 
				Configuration config = new Configuration();

//				// Get the filesystem - HDFS
				FileSystem file_sys = FileSystem.get(config);
				
				long output_file_size = file_sys.getContentSummary(outFile).getSpaceConsumed();

				
				System.out.println("DATE :" + " " + Afterdate);
				System.out.println("SIZE OF THE INPUT FILE :" + fileSize + " " + "Bytes");
				System.out.println("SIZE OF THE OUTPUT FILE :" + file_sys.getContentSummary(outFile).getSpaceConsumed()+ " " + "Bytes");

				System.out.println("INPUT PATH :" + " " + intput_file_path);
				System.out.println("OUTPUT HDFS PATH :" + " " + saved_file_path);

				
				String Input_file_name = intput_file_path;
				if(intput_file_path.indexOf("/") != -1) {
					Input_file_name = intput_file_path.substring(intput_file_path.lastIndexOf("/") + 1);
//					System.out.println("INPUT FILE NAME :" + " " + Input_file_name);

				}
				System.out.println("INPUT FILE NAME :" + " " + Input_file_name);
				
				String Output_file_name = saved_file_path;
				if(saved_file_path.indexOf("/") != -1) {
					Output_file_name = saved_file_path.substring(saved_file_path.lastIndexOf("/") + 1);
//					System.out.println("OUTPUT FILE NAME :" + " " + Output_file_name);

				}
				System.out.println("OUTPUT FILE NAME :" + " " + Output_file_name);

				
//				FSDataInputStream input = null;
//
//				try {
//					// Open the path mentioned in HDFS
//					input = file_sys.open(new Path(uri));
//					IOUtils.copyBytes(input, System.out, 4096, false);
//
//					System.out.println("End Of file: HDFS file read complete");
//
//				} finally {
//					IOUtils.closeStream(input);
//				}
//				
				
								
				// Register driver and create driver instance
		      try {
			     Class.forName(driverName);
		      }
		      catch(ClassNotFoundException ex){
		    	  System.out.println("DRIVER CLASS NOT FOUND .....");
		      }


			  // get connection
//			  Connection con = DriverManager.getConnection("jdbc:hive2://", "", "");
			  Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
			  Statement stmt = (Statement) con.createStatement();
//			  ResultSet test = stmt.executeQuery("CREATE DATABASE userdb2");
//			  System.out.println("Database userdb created successfully.");
			  

		      try {
			      boolean create_database = stmt.execute("CREATE DATABASE IF NOT EXISTS compression_datas");
//			      ResultSet create_database = stmt.executeQuery("CREATE DATABASE IF NOT EXISTS compression_datas");
			      System.out.println("Database compression_datas created successfully.");
			      }catch (Exception exception){
			    	  exception.printStackTrace();
			      }
			      
			  try {
				  boolean create_table = stmt.execute("CREATE TABLE IF NOT EXISTS "
			    	      +" compression_datas.compression_file_details ( id INT, file_name STRING, "
			    	      +" file_path STRING, file_size BIGINT)stored as ORC TBLPROPERTIES ('transaction'='true')");
			      
			      System.out.println("File Details Table created." + create_table);
			      }catch (Exception exception){
			    	  exception.printStackTrace();
			      }	  
			  
//			  boolean check_table_ispresent = stmt.execute("Select * from compression_datas.compression_file_details");
//			  System.out.println("CHECK TABLE :"+ check_table_ispresent);
//			 
//			  
//			  boolean check_table_present = stmt.execute("SHOW TABLES IN compression_datas LIKE 'compression_file'");
//			  System.out.println("CHECK TABLE IS PRESENT OR NOT :"+ check_table_present);
//
//			  
//			  ResultSet check_table = stmt.executeQuery("Select * from compression_datas.compression_file_details");
//			  System.out.println("CHECK TABLE :"+ check_table);
//			  if(check_table != null){
//				  System.out.println(" TABLE CREATED");
//				  }
//			  
			  try {
				  boolean data_insert = stmt.execute("INSERT INTO table "
					  + " compression_datas.compression_file_details VALUES "
					  + "(2,"+"'"+Output_file_name +"'"+','+"'"+ saved_file_path +"'"+ ','+ output_file_size +")" );					  
					  
			  }catch (Exception exception){
		    	  exception.printStackTrace();
		      }
			  
			  ResultSet res = stmt.executeQuery("select * from compression_datas.compression_file_details");
			  System.out.println("TABLE DATAS :"+ res);

//			  String sql= "SHOW TABLES compression_file_details";
//			  System.out.println("SHOW TABLE :"+ sql);
//			  ResultSet resut=stmt.executeQuery(sql);
			  
			  con.close();

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
