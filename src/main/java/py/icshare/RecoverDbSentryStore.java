package py.icshare;

import java.util.List;

public interface RecoverDbSentryStore {

  List<RecoverDbSentry> list();

  void saveOrUpdate(RecoverDbSentry recoverDbSentry);

  public void clearAll();
}
