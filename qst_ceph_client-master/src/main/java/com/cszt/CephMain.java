package com.cszt;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.cszt.CephClient;
import com.cszt.util.CephException;


public class CephMain {
	public static void main (String[] args) {
		String fileName = args[1];
		try {
			CephClient.init(); // 能且只能执行一次
		} catch (CephException e) {
			e.printStackTrace();
			System.out.print(e.getMsg());
		}
		String mode = args[0];
		if(mode.equals("auto")) {
			CephClient.preCreateBucket(); // 能且只能执行一次
			String bucketName = CephClient.getBucketName(args[3]);
			String path = args[2];
			String jobName = args[3];
			String birth = args[4];
			int days = Integer.valueOf(args[5]);
			try {
				CephClient.putObject(bucketName,path, fileName, jobName, birth);
				CephClient.changeAllRule(days);
			} catch (CephException e) {
				e.printStackTrace();
				System.out.print(e.getMsg());
			}
		}
		if(mode.equals("manual")){
			String bucketName = args[2];
			String path = args[3];
			String jobName = args[4];
			String birth = args[5];
			int days = Integer.valueOf(args[6]);

			CephClient.createBucket(bucketName);
			try {
				CephClient.putObject(bucketName, path, fileName, jobName, birth);
				CephClient.changeRule(bucketName, days);
			} catch (CephException e) {
				e.printStackTrace();
				System.out.print(e.getMsg());
			}
		}

		try {
			CephClient.deleteMultiObject("job1");
			CephClient.deleteMultiObject("job2", "20190530");
		} catch (CephException e) {
			e.printStackTrace();
			System.out.print(e.getMsg());
		}

	}
}

//C:\Users\XW\Desktop\qst_ceph_client-master\src\main\java\com\cszt\util