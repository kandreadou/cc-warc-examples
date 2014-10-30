package org.commoncrawl.mklab.analysis;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.*;

/**
 * Created by kandreadou on 10/30/14.
 */
public class DownloadService {

    private static final int NUM_DOWNLOAD_THREADS = 10;
    private static final int MAX_NUM_PENDING_TASKS = 10 * NUM_DOWNLOAD_THREADS;
    private static final int CONNECTION_TIMEOUT = 5000; // in millis
    private static final int READ_TIMEOUT = 5000; // in millis
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_DOWNLOAD_THREADS);
    private final CompletionService<Result> service = new ExecutorCompletionService<Result>(executor);

    private int numPendingTasks;

    public void submitTask(String url) {
        Callable<Result> call = new Download(url);
        service.submit(call);
        numPendingTasks++;
    }

    public Result tryGetResult() {
        try {
            numPendingTasks--;
            Future<Result> future = service.poll();
            if(future!=null)
                return future.get();
            return null;
        }catch(Exception e){
            System.out.println("Exception in getResultWait: "+e);
            return null;
        }
    }

    public boolean canAcceptMoreTasks() {
        return numPendingTasks < MAX_NUM_PENDING_TASKS;
    }

    public void printStatus(){
        System.out.println("Pending tasks: "+numPendingTasks);
    }

    public void shutDown() {
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    class Download implements Callable<Result> {

        private String imageUrl;

        public Download(String imageUrl) {
            this.imageUrl = imageUrl;
        }

        @Override
        public Result call() throws Exception {
            HttpURLConnection conn = null;
            boolean success = false;
            try {
                URL url = new URL(imageUrl);
                conn = (HttpURLConnection) url.openConnection();
                conn.setInstanceFollowRedirects(true);
                conn.setConnectTimeout(CONNECTION_TIMEOUT); // TO DO: add retries when connections times out
                conn.setReadTimeout(READ_TIMEOUT);
                conn.connect();
                //System.out.println("Content length: " + conn.getContentLength() + " Content type: " + conn.getContentType());
                success = true;
            } catch (Exception e) {
                System.out.println("Exception at url: " + imageUrl);
            } finally {
                Result result;
                if (conn != null) {
                    result = new Result(imageUrl, success, conn.getContentLength(), conn.getContentType());
                    conn.disconnect();
                } else {
                    result = new Result(imageUrl, false);
                }
                return result;
            }
        }
    }

    public class Result {
        public String url;
        public boolean success = false;
        public int contentLength;
        public String contentType;

        public Result(String url, boolean success) {
            this.url = url;
            this.success = success;
        }

        public Result(String url, boolean success, int contentLength, String contentType) {
            this.url = url;
            this.success = success;
            this.contentLength = contentLength;
            this.contentType = contentType;
        }

    }
}
