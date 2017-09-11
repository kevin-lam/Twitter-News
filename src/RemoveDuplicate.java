
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;




public class RemoveDuplicate {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder ="F:/cs242/";
		File f= new File(folder);
		String output="F:/CS242data.txt";
		OutputStreamWriter osw= null;
		Set<String>  hs= new HashSet<String>(); 
		try{
			osw = new OutputStreamWriter(new FileOutputStream(output,true));
			String[] list = f.list();
			for(int i=0;i<list.length;i++)
			{
				System.out.println(list[i]);
				BufferedReader buff=new BufferedReader(new FileReader(folder+list[i]));
				String lines="";
				
				while(((lines=buff.readLine())!=null))
				{
					hs.add(lines);

				}
				buff.close();
			}
			
			Iterator<String> it = hs.iterator();
			while(it.hasNext()){
				osw.write(it.next()+"\n");
			}
			osw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

}
