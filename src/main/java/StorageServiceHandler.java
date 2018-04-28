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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class StorageServiceHandler implements Storage.AsyncIface{

    private final static Logger logger = Logger.getLogger(StorageServiceHandler.class);


    private DatabaseReference ref;

    private UpdateChapterListListener updateChapterListListener;
    private UpdateChapterListener updateChapterListener;

    private ArrayList<Callback> unsentBookList;
    private AtomicBoolean bookListSent = new AtomicBoolean(false);
    private static final Object bookListLock = new Object();

    private ArrayList<Callback> unsentChapterList;
    private AtomicBoolean chapterListSent = new AtomicBoolean(false);
    private static final Object chapterListLock = new Object();

    private ArrayList<Callback> unsentChapterInfoList;
    private AtomicBoolean chapterInfoListSent = new AtomicBoolean(false);
    private static final Object chapterInfoListLock = new Object();


    StorageServiceHandler() {
        try {
            initializeApp();
            ref = FirebaseDatabase.getInstance()
                    .getReference().child("books");
            createEventListeners();
            createCash();
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

    private void createCash() {
        unsentBookList = new ArrayList<>();
        unsentChapterList = new ArrayList<>();
        unsentChapterInfoList = new ArrayList<>();
    }


    @Override
    public void getBookList(AsyncMethodCallback<Callback> resultHandler) {
        logger.info("Called getBookList()");
        this.clearBookCash();
        ref.addChildEventListener(new UpdateBookListListener(resultHandler));
    }

    private void clearBookCash() {
        unsentBookList = new ArrayList<>();
        bookListSent.set(false);
        this.clearChapterCash();
    }

    private void clearChapterCash() {
        unsentChapterList = new ArrayList<>();
        chapterListSent.set(false);
        this.clearChapterInfoCash();
    }

    @Override
    public void getBookChapters(String bookUid, AsyncMethodCallback<Callback> resultHandler) {
        logger.info("Called getBookChapters(" + bookUid + ")");
        DatabaseReference currentBookChaptersRef = ref.child(bookUid).child("chapters");
        this.clearChapterCash();
        currentBookChaptersRef.removeEventListener(updateChapterListListener);
        updateChapterListListener = new UpdateChapterListListener(resultHandler);
        currentBookChaptersRef.addChildEventListener(updateChapterListListener);
    }

    private void clearChapterInfoCash() {
        unsentChapterInfoList = new ArrayList<>();
        chapterInfoListSent.set(false);
    }

    @Override
    public void renameBook(CustomPair newValue, AsyncMethodCallback<Void> resultHandler) {
        String key = newValue.getKey();
        String newName = newValue.getValue();
        ref.child(key)
                .child("name")
                .setValueAsync(newName);
    }

    @Override
    public void renameBookChapter(String bookUid, CustomPair newValue, AsyncMethodCallback<Void> resultHandler) {
        String key = newValue.getKey();
        String newName = newValue.getValue();
        ref.child(bookUid)
                .child("chapters")
                .child(key)
                .child("name")
                .setValueAsync(newName);
    }

    @Override
    public void getChapter(String bookUid, String chapterUid, AsyncMethodCallback<Callback> resultHandler) {
        DatabaseReference chaptersRef =  ref.child(bookUid)
                .child("chapters")
                .child(chapterUid);
        this.clearChapterInfoCash();
        chaptersRef.removeEventListener(updateChapterListener);
        updateChapterListener = new UpdateChapterListener(resultHandler);
        chaptersRef.addChildEventListener(updateChapterListener);
    }

    @Override
    public void updateChapter(String bookUid, String chapterUid, Chapter chapter, AsyncMethodCallback<Void> resultHandler) {
        ref.child(bookUid)
                .child("chapters")
                .child(chapterUid)
                .setValueAsync(chapter);
    }

    @Override
    public void removeBook(String bookUid, AsyncMethodCallback<Void> resultHandler) {
        ref.child(bookUid)
                .removeValueAsync();
    }

    @Override
    public void removeChapter(String bookUid, String chapterUid, AsyncMethodCallback<Void> resultHandler) {
        ref.child(bookUid)
                .child("chapters")
                .child(chapterUid)
                .removeValueAsync();
    }

    @Override
    public void addBook(String bookName, AsyncMethodCallback<Void> resultHandler) {
        ref.push()
                .child("name")
                .setValueAsync(bookName);
    }

    @Override
    public void addChapter(String bookUid, String chapterName, AsyncMethodCallback<Void> resultHandler) {
        ref.child(bookUid)
                .child("chapters")
                .push()
                .child("name")
                .setValueAsync(chapterName);
    }

    @Override
    public void subscribeForBookList(AsyncMethodCallback<List<Callback>> resultHandler) {
        this.notifySubscriber(bookListLock, bookListSent, unsentBookList, resultHandler);
    }

    @Override
    public void subscribeForBookChapters(String bookUid, AsyncMethodCallback<List<Callback>> resultHandler) throws TException {
        this.notifySubscriber(chapterListLock, chapterListSent, unsentChapterList, resultHandler);
    }

    @Override
    public void subscribeForChapter(String bookUid, String chapterUid, AsyncMethodCallback<List<Callback>> resultHandler) throws TException {
        this.notifySubscriber(chapterInfoListLock, chapterInfoListSent, unsentChapterInfoList, resultHandler);
    }

    private void notifySubscriber(Object lock,
                                  AtomicBoolean arraySent,
                                  ArrayList<Callback> array,
                                  AsyncMethodCallback<List<Callback>> resultHandler) {
        synchronized (lock) {
            if (arraySent.get() && !array.isEmpty()) {
                resultHandler.onComplete(array);
                arraySent.set(true);
            } else  {
                //resultHandler.onComplete(null);
            }
        }
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
            send(bookListLock,
                    bookListSent,
                    new Callback(entry, "books", CallbackType.ADDED, null),
                    ValidUpdate.BOOK_NAME,
                    unsentBookList,
                    callback);
        }

        @Override
        public void onChildChanged(DataSnapshot dataSnapshot, String s) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Changed book name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            send(bookListLock,
                    bookListSent,
                    new Callback(entry, "books", CallbackType.CHANGED, null),
                    ValidUpdate.BOOK_NAME,
                    unsentBookList,
                    callback);
        }

        @Override
        public void onChildRemoved(DataSnapshot dataSnapshot) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Removed book name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            send(bookListLock,
                    bookListSent,
                    new Callback(entry, "books", CallbackType.REMOVED, null),
                    ValidUpdate.BOOK_NAME,
                    unsentBookList,
                    callback);
        }

        @Override
        public void onChildMoved(DataSnapshot dataSnapshot, String s) {

        }

        @Override
        public void onCancelled(DatabaseError databaseError) {
            callback.onError(databaseError.toException());
        }

    }

    private void send(Object lock,
                      AtomicBoolean arraySent,
                      Callback callbackInfo,
                      ValidUpdate update,
                      ArrayList<Callback> unsent,
                      AsyncMethodCallback<Callback> callback) {
        synchronized (lock) {
            if (!arraySent.get()) {
                callback.onComplete(callbackInfo);
                arraySent.set(true);
            } else  {
                callbackInfo.update = update;
                unsent.add(callbackInfo);
            }
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
            send(chapterListLock,
                    chapterListSent,
                    new Callback(entry, "chapters", CallbackType.ADDED, null),
                    ValidUpdate.CHAPTER_NAME,
                    unsentChapterList,
                    callback);
        }

        @Override
        public void onChildChanged(DataSnapshot dataSnapshot, String s) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Changed chapter name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            send(chapterListLock,
                    chapterListSent,
                    new Callback(entry, "chapters", CallbackType.CHANGED, null),
                    ValidUpdate.CHAPTER_NAME,
                    unsentChapterList,
                    callback);
        }

        @Override
        public void onChildRemoved(DataSnapshot dataSnapshot) {
            String name = (String) dataSnapshot.child("name").getValue();
            String key = dataSnapshot.getKey();
            logger.info("Removed chapter name: " + name + ", key: " + key);
            CustomPair entry = new CustomPair(key, name);
            send(chapterListLock,
                    chapterListSent,
                    new Callback(entry, "chapters", CallbackType.REMOVED, null),
                    ValidUpdate.CHAPTER_NAME,
                    unsentChapterList,
                    callback);
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
            logger.info("Added chapter event: " + event + ", value: " + value);
            send(chapterInfoListLock,
                    chapterInfoListSent,
                    new Callback(entry, event, CallbackType.ADDED, null),
                    ValidUpdate.CHAPTER_INFO,
                    unsentChapterInfoList,
                    callback);
        }

        @Override
        public void onChildChanged(DataSnapshot dataSnapshot, String s) {
            String value = (String) dataSnapshot.getValue();
            String event = dataSnapshot.getKey();
            CustomPair entry = new CustomPair(event, value);
            logger.info("Changed chapter event: " + event + ", value: " + value);
            send(chapterInfoListLock,
                    chapterInfoListSent,
                    new Callback(entry, event, CallbackType.CHANGED, null),
                    ValidUpdate.CHAPTER_INFO,
                    unsentChapterInfoList,
                    callback);
        }

        @Override
        public void onChildRemoved(DataSnapshot dataSnapshot) {
            String value = (String) dataSnapshot.getValue();
            String event = dataSnapshot.getKey();
            CustomPair entry = new CustomPair(event, value);
            logger.info("Removed chapter event: " + event + ", value: " + value);
            send(chapterInfoListLock,
                    chapterInfoListSent,
                    new Callback(entry, event, CallbackType.REMOVED, null),
                    ValidUpdate.CHAPTER_INFO,
                    unsentChapterInfoList,
                    callback);
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
