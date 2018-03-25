import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.*;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import thrift.*;

import java.io.FileInputStream;
import java.io.IOException;

public class StorageServiceHandler implements Storage.AsyncIface{

    private final static Logger logger = Logger.getLogger(StorageServiceHandler.class);


    private DatabaseReference ref;

    private UpdateChapterListListener updateChapterListListener;
    private UpdateChapterListener updateChapterListener;

    StorageServiceHandler() {
        try {
            initializeApp();
            ref = FirebaseDatabase.getInstance()
                    .getReference().child("books");
            createEventListeners();
        } catch (IOException e) {
            logger.error("Error in app initialization: " + e.getLocalizedMessage());
        }
    }

    private void initializeApp() throws IOException {
        FileInputStream serviceAccount = new FileInputStream("src/main/resources/realtimehandbookservce-firebase-adminsdk-ty3to-24c90c1cac.json");

        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                .setDatabaseUrl("https://realtimehandbookservce.firebaseio.com/")
                .build();

        FirebaseApp.initializeApp(options);

        logger.info("Application initialized successfully");
    }

    private void createEventListeners() {
        updateChapterListListener = new UpdateChapterListListener(null);
        updateChapterListener = new UpdateChapterListener(null);
    }


    @Override
    public void getBookList(AsyncMethodCallback<Callback> resultHandler) throws TException {
        logger.info("Called getBookList()");
        ref.addChildEventListener(new UpdateBookListListener(resultHandler));
    }

    @Override
    public void getBookChapters(String bookUid, AsyncMethodCallback<Callback> resultHandler) throws TException {
        logger.info("Called getBookChapters(" + bookUid + ")");
        DatabaseReference currentBookChaptersRef = ref.child(bookUid).child("chapters");
        currentBookChaptersRef.removeEventListener(updateChapterListListener);
        updateChapterListListener = new UpdateChapterListListener(resultHandler);
        currentBookChaptersRef.addChildEventListener(updateChapterListListener);
    }

    @Override
    public void renameBook(CustomPair newValue, AsyncMethodCallback<Void> resultHandler) throws TException {
        String key = newValue.getKey();
        String newName = newValue.getValue();
        ref.child(key)
                .child("name")
                .setValueAsync(newName);
    }

    @Override
    public void renameBookChapter(String bookUid, CustomPair newValue, AsyncMethodCallback<Void> resultHandler) throws TException {
        String key = newValue.getKey();
        String newName = newValue.getValue();
        ref.child(bookUid)
                .child("chapters")
                .child(key)
                .child("name")
                .setValueAsync(newName);
    }

    @Override
    public void getChapter(String bookUid, String chapterUid, AsyncMethodCallback<Callback> resultHandler) throws TException {
        DatabaseReference chaptersRef =  ref.child(bookUid)
                .child("chapters")
                .child(chapterUid);
        chaptersRef.removeEventListener(updateChapterListener);
        updateChapterListener = new UpdateChapterListener(resultHandler);
        chaptersRef.addChildEventListener(updateChapterListener);
    }

    @Override
    public void updateChapter(String bookUid, String chapterUid, Chapter chapter, AsyncMethodCallback<Void> resultHandler) throws TException {
        ref.child(bookUid)
                .child("chapters")
                .child(chapterUid)
                .setValueAsync(chapter);
    }

    @Override
    public void removeBook(String bookUid, AsyncMethodCallback<Void> resultHandler) throws TException {
        ref.child(bookUid)
                .removeValueAsync();
    }

    @Override
    public void removeChapter(String bookUid, String chapterUid, AsyncMethodCallback<Void> resultHandler) throws TException {
        ref.child(bookUid)
                .child("chapters")
                .child(chapterUid)
                .removeValueAsync();
    }

    @Override
    public void addBook(String bookName, AsyncMethodCallback<Void> resultHandler) throws TException {
        ref.push()
                .child("name")
                .setValueAsync(bookName);
    }

    @Override
    public void addChapter(String bookUid, String chapterName, AsyncMethodCallback<Void> resultHandler) throws TException {
        ref.child(bookUid)
                .child("chapters")
                .push()
                .child("name")
                .setValueAsync(chapterName);
    }


    private class UpdateBookListListener implements ChildEventListener {

        private final AsyncMethodCallback<Callback> callback;

        UpdateBookListListener(AsyncMethodCallback<Callback> callback) {
            this.callback  = callback;
        }

        @Override
        public void onChildAdded(DataSnapshot dataSnapshot, String s) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            CustomPair entry = new CustomPair(key, name);
            logger.info("Added book: " + name);
            callback.onComplete(new Callback(entry, "books", CallbackType.ADDED));
        }

        @Override
        public void onChildChanged(DataSnapshot dataSnapshot, String s) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Changed book name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            callback.onComplete(new Callback(entry, "books", CallbackType.CHANGED));
        }

        @Override
        public void onChildRemoved(DataSnapshot dataSnapshot) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Removed book name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            callback.onComplete(new Callback(entry, "books", CallbackType.REMOVED));
        }

        @Override
        public void onChildMoved(DataSnapshot dataSnapshot, String s) {

        }

        @Override
        public void onCancelled(DatabaseError databaseError) {
            callback.onError(databaseError.toException());
        }
    }

    private class UpdateChapterListListener implements ChildEventListener {

        private final AsyncMethodCallback<Callback> callback;

        UpdateChapterListListener(AsyncMethodCallback<Callback> callback) {
            this.callback  = callback;
        }

        @Override
        public void onChildAdded(DataSnapshot dataSnapshot, String s) {
            Chapter chapter = dataSnapshot.getValue(Chapter.class);
            String key = dataSnapshot.getKey();
            String name = chapter.getName();
            logger.info("Added chapter: " + name);
            CustomPair entry = new CustomPair(key, name);
            callback.onComplete(new Callback(entry, "chapters", CallbackType.ADDED));
        }

        @Override
        public void onChildChanged(DataSnapshot dataSnapshot, String s) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Changed chapter name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            callback.onComplete(new Callback(entry, "chapters", CallbackType.CHANGED));
        }

        @Override
        public void onChildRemoved(DataSnapshot dataSnapshot) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Removed chapter name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            callback.onComplete(new Callback(entry, "chapters", CallbackType.REMOVED));
        }

        @Override
        public void onChildMoved(DataSnapshot dataSnapshot, String s) {

        }

        @Override
        public void onCancelled(DatabaseError databaseError) {
            callback.onError(databaseError.toException());
        }
    }

    private class UpdateChapterListener implements ChildEventListener {

        private final AsyncMethodCallback<Callback> callback;

        UpdateChapterListener(AsyncMethodCallback<Callback> callback) {
            this.callback  = callback;
        }

        @Override
        public void onChildAdded(DataSnapshot dataSnapshot, String s) {
            String value = (String) dataSnapshot.getValue();
            String event = dataSnapshot.getKey();
            CustomPair entry = new CustomPair(event, value);
            callback.onComplete(new Callback(entry, event, CallbackType.ADDED));
        }

        @Override
        public void onChildChanged(DataSnapshot dataSnapshot, String s) {
            String value = (String) dataSnapshot.getValue();
            String event = dataSnapshot.getKey();
            CustomPair entry = new CustomPair(event, value);
            callback.onComplete(new Callback(entry, event, CallbackType.CHANGED));
        }

        @Override
        public void onChildRemoved(DataSnapshot dataSnapshot) {
            String value = (String) dataSnapshot.getValue();
            String event = dataSnapshot.getKey();
            CustomPair entry = new CustomPair(event, value);
            callback.onComplete(new Callback(entry, event, CallbackType.REMOVED));
        }

        @Override
        public void onChildMoved(DataSnapshot dataSnapshot, String s) {

        }

        @Override
        public void onCancelled(DatabaseError databaseError) {
            callback.onError(databaseError.toException());
        }

    }

}
