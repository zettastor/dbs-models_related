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

package py.common.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.test.TestBase;

public class ClientFactoryTester extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(ClientFactoryTester.class);

  @Test
  public void test() throws Exception {
    Tc<Ta, Tb> tc = new Tc<Ta, Tb>(Ta.class, Tb.class);
    try {
      // tc.getType1Constructor();
      tc.getType2Constructor();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
    }
  }

  private static class Ta {
    public Ta() {
    }

    public Ta(Integer a) {
    }
  }

  private static class Tb extends Ta {
    public Tb() {
    }

    public Tb(Integer m) {
    }

    public void test() {
    }
  }

  public static class Tc<Type1T, Type2T> {
    private Class<? extends Type1T> taClass;
    private Class<? extends Type2T> tbClass;

    public Tc(Class<? extends Type1T> taClass, Class<? extends Type2T> tbClass) {
      this.taClass = taClass;
      this.tbClass = tbClass;
    }

    public Type1T getType1Constructor() throws Exception {
      Constructor<? extends Type1T> taConstructor = taClass.getDeclaredConstructor(Integer.class);
      return taConstructor.newInstance();
    }

    public Type2T getType2Constructor() throws Exception {
      logger.debug("Class is {}", tbClass.getName());
      for (Method method : tbClass.getMethods()) {
        logger.debug("sssssssssss {} ", method);
      }

      Constructor<?>[] constructors = tbClass.getConstructors();
      for (Constructor<?> constructor : constructors) {
        logger.debug("Constructors is {}", constructor);
      }
      Constructor<? extends Type2T> tbConstructor = tbClass.getConstructor();
      return tbConstructor.newInstance();
    }
  }

  public static class Td<Type1T, Type2T> extends Tc<Type1T, Type2T> {
    public Td(Class<? extends Type1T> taClass, Class<? extends Type2T> tbClass) {
      super(taClass, tbClass);

    }
  }
}
