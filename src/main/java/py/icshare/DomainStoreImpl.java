/*
 * Copyright (c) 2022-2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.icshare;

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class DomainStoreImpl implements DomainStore, DomainDbStore {
  private static final Logger logger = LoggerFactory.getLogger(DomainStoreImpl.class);
  private SessionFactory sessionFactory;
  private Map<Long, Domain> domainMap = new ConcurrentHashMap<Long, Domain>();

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void saveDomainToDb(Domain domain) {
    if (domain == null) {
      logger.warn("Invalid param, please check them");
      return;
    }

    DomainInformationDb domainInformation = domain.toDomainInformationDb(this);
    sessionFactory.getCurrentSession().saveOrUpdate(domainInformation);
  }

  @Override
  public void deleteDomainFromDb(Long domainId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete DomainInformationDb where domainId = :domainId");
    query.setLong("domainId", domainId);
    query.executeUpdate();
  }

  @Override
  public Domain getDomainFromDb(Long domainId) throws SQLException, IOException {
    DomainInformationDb domainInformation = (DomainInformationDb) sessionFactory.getCurrentSession()
        .get(
            DomainInformationDb.class, domainId);
    if (domainInformation == null) {
      return null;
    }

    return domainInformation.toDomain();
  }

  @Override
  public boolean saveDomain(Domain domain) {
    if (domain == null) {
      logger.warn("Invalid param, please check them");
      return false;
    }
    // save to memory
    domainMap.put(domain.getDomainId(), domain);
    // save to DB
    saveDomainToDb(domain);
    return true;
  }

  @Override
  public void deleteDomain(Long domainId) {
    // delete from memory
    domainMap.remove(domainId);
    // delete from DB
    deleteDomainFromDb(domainId);
  }

  @Override
  public Domain getDomain(Long domainId) throws SQLException, IOException {
    Domain domain = domainMap.get(domainId);
    if (domain == null) {
      domain = getDomainFromDb(domainId);
    }
    return domain;
  }

  @Override
  public List<Domain> listAllDomains() throws SQLException, IOException {
    List<Domain> allDomains = new ArrayList<Domain>();
    // if infocenter restart, should reload domains from DB
    if (domainMap.isEmpty()) {
      reloadAllDomainsFromDb();
    }
    allDomains.addAll(domainMap.values());
    return allDomains;
  }

  @Override
  public List<Domain> listDomains(List<Long> domainIds) throws SQLException, IOException {
    List<Domain> domains = new ArrayList<Domain>();
    // if infocenter restart, should reload domains from DB
    if (domainMap.isEmpty()) {
      reloadAllDomainsFromDb();
    }
    for (Long domainId : domainIds) {
      if (domainMap.containsKey(domainId)) {
        domains.add(domainMap.get(domainId));
      }
    }
    return domains;
  }

  @Override
  public void reloadAllDomainsFromDb() throws SQLException, IOException {
    @SuppressWarnings("unchecked")
    List<DomainInformationDb> allDomainsInDb = sessionFactory.getCurrentSession()
        .createQuery("from DomainInformationDb").list();
    for (DomainInformationDb domainInformation : allDomainsInDb) {
      domainMap.put(domainInformation.getDomainId(), domainInformation.toDomain());
    }
  }

  @Override
  public void clearMemoryMap() {
    domainMap.clear();
  }

  @Override
  public void removeDatanodeFromDomain(Long domainId, Long datanodeInstanceId)
      throws SQLException, IOException {
    Domain domain = getDomain(domainId);
    if (domain == null) {
      logger.warn("do not have domain:{}", domainId);
      return;
    }
    domain.getDataNodes().remove(datanodeInstanceId);
    saveDomain(domain);
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }
}
