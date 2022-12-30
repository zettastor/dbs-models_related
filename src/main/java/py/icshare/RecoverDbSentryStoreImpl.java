package py.icshare;

import java.util.List;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

public class RecoverDbSentryStoreImpl implements RecoverDbSentryStore {

  private static final Logger logger = LoggerFactory.getLogger(RecoverDbSentryStoreImpl.class);
  private SessionFactory sessionFactory;

  @Transactional(readOnly = true)
  @Override
  public List<RecoverDbSentry> list() {
    return sessionFactory.getCurrentSession().createQuery("from RecoverDbSentry")
        .list();
  }

  @Transactional(rollbackFor = Exception.class)
  @Override
  public void saveOrUpdate(RecoverDbSentry recoverDbSentry) {
    sessionFactory.getCurrentSession().saveOrUpdate(recoverDbSentry);
  }

  @Transactional
  @Override
  public void clearAll() {
    sessionFactory.getCurrentSession().createQuery("delete from RecoverDbSentry")
            .executeUpdate();
  }

  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}
