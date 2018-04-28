namespace java thrift

struct Handbook {
    1: string name;
    2: list<Chapter> chapters;
}

struct Chapter {
    1: string name;
    2: string description
    3: string text;
}

struct CustomPair {
    1: string key;
    2: string value;
}

struct Callback {
    1: CustomPair pair;
    2: string event;
    3: CallbackType type;
    4: ValidUpdate update;
}

enum CallbackType {
    ADDED,
    CHANGED,
    REMOVED
}

enum ValidUpdate {
    BOOK_NAME,
    CHAPTER_NAME,
    CHAPTER_INFO
}


service Storage {

    Callback getBookList();
    Callback getBookChapters(1: string bookUid);
    void renameBook(1: CustomPair newValue);
    void renameBookChapter(1: string bookUid, 2: CustomPair newValue);
    Callback getChapter(1: string bookUid, 2: string chapterUid);
    void updateChapter(1: string bookUid, 2: string chapterUid, 3: Chapter chapter);
    void removeBook(1: string bookUid);
    void removeChapter(1: string bookUid, 2: string chapterUid);
    void addBook(1: string bookName);
    void addChapter(1: string bookUid, 2: string chapterName)


    list<Callback>  subscribeForBookList();
    list<Callback>  subscribeForBookChapters(1: string bookUid);
    list<Callback>  subscribeForChapter(1: string bookUid, 2: string chapterUid);
}
