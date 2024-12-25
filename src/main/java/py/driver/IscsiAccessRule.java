/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.driver;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IscsiAccessRule {
  private static final Logger logger = LoggerFactory.getLogger(IscsiAccessRule.class);
  /**
   * for below fields, corresponding to each other in targetcli.
   *
   * <li>{@link IscsiAccessRule#initiatorName}  wwn</li>
   * <li>{@link IscsiAccessRule#incomingUser}   auth userid</li>
   * <li>{@link IscsiAccessRule#incomingPasswd} auth password</li>
   * <li>{@link IscsiAccessRule#outgoingUser}   auth mutual_userid</li>
   * <li>{@link IscsiAccessRule#outgoingPasswd} auth mutual_password</li>
   */
  public String initiatorName;
  public String incomingUser;
  public String incomingPasswd;
  public String outgoingUser;
  public String outgoingPasswd;

  public IscsiAccessRule() {
  }

  public IscsiAccessRule(String initiatorName, String incomingUser, String incomingPasswd,
      String outgoingUser, String outgoingPasswd) {
    this.initiatorName = initiatorName;
    this.incomingUser = incomingUser;
    this.incomingPasswd = incomingPasswd;
    this.outgoingUser = outgoingUser;
    this.outgoingPasswd = outgoingPasswd;
  }

  public static Logger getLogger() {
    return logger;
  }

  public String getInitiatorName() {
    return initiatorName;
  }

  public void setInitiatorName(String initiatorName) {
    this.initiatorName = initiatorName;
  }

  public String getIncomingUser() {
    return incomingUser;
  }

  public void setIncomingUser(String incomingUser) {
    this.incomingUser = incomingUser;
  }

  public String getIncomingPasswd() {
    return incomingPasswd;
  }

  public void setIncomingPasswd(String incomingPasswd) {
    this.incomingPasswd = incomingPasswd;
  }

  public String getOutgoingUser() {
    return outgoingUser;
  }

  public void setOutgoingUser(String outgoingUser) {
    this.outgoingUser = outgoingUser;
  }

  public String getOutgoingPasswd() {
    return outgoingPasswd;
  }

  public void setOutgoingPasswd(String outgoingPasswd) {
    this.outgoingPasswd = outgoingPasswd;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IscsiAccessRule)) {
      return false;
    }
    IscsiAccessRule that = (IscsiAccessRule) o;
    return Objects.equals(getInitiatorName(), that.getInitiatorName()) && Objects
        .equals(getIncomingUser(), that.getIncomingUser()) && Objects
        .equals(getIncomingPasswd(), that.getIncomingPasswd()) && Objects
        .equals(getOutgoingUser(), that.getOutgoingUser()) && Objects
        .equals(getOutgoingPasswd(), that.getOutgoingPasswd());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(getInitiatorName(), getIncomingUser(), getIncomingPasswd(), getOutgoingUser(),
            getOutgoingPasswd());
  }

  @Override
  public String toString() {
    return "IscsiAccessRule{" + "initiatorName='" + initiatorName + '\'' + ", incomingUser='"
        + incomingUser + '\''
        + ", incomingPasswd='" + incomingPasswd + '\'' + ", outgoingUser='" + outgoingUser + '\''
        + ", outgoingPasswd='" + outgoingPasswd + '\'' + '}';
  }
}
