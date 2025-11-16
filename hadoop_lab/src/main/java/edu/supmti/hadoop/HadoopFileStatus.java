package edu.supmti.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            Path filepath = new Path(args[0], args[1]);
            FileStatus info = fs.getFileStatus(filepath);
            if (!fs.exists(filepath)) {
                System.out.println("File does not exists");
                System.exit(1);
            }
            System.out.println(Long.toString(info.getLen()) + " bytes");
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + info.getLen());
            System.out.println("File owner: " + info.getOwner());
            System.out.println("File permission: " + info.getPermission());
            System.out.println("File Replication: " + info.getReplication());
            System.out.println("File Block Size: " + info.getBlockSize());
            BlockLocation[] blockLocations = fs.getFileBlockLocations(info, 0, info.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            fs.rename(filepath, new Path(args[0], args[2]));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
