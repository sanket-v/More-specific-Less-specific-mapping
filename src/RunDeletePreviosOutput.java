import java.io.File;
import java.io.IOException;

public class RunDeletePreviosOutput {
	static int files_deleted;
	public static void main(String []args) throws IOException {
		String location = "/home/sanketv/Output of Eclipse projects/Output of MoreLessSpecific";
		File root_dir = new File(location);
		files_deleted = 0;
		directoryTree(root_dir);
		System.out.println(files_deleted+" files deleted from "+location);
	}

	private static void directoryTree(File directory) {
		// TODO Auto-generated method stub
		File [] files = directory.listFiles();

		// traversing array of current directory
		for(File file : files){
			// if current file is directory recursion call
			if(file.isDirectory())
				directoryTree(file);

			if(file.isFile()) {
				file.delete();
				files_deleted++;
			}
		}
	}
}
