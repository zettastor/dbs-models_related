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

package py.common.client;

import java.lang.reflect.Field;
import java.util.UUID;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.aop.TestRetryService;
import py.aop.TestRetryServiceImpl;
import py.aop.TestRetryServiceWrapper;
import py.client.GenericProxyFactory;
import py.test.TestBase;

public class DynamicProxyPerformanceTester extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(DynamicProxyPerformanceTester.class);

  private static void testDelegate(TestRetryService service, String label) throws Exception {
    service.testPerformance(); // warm up
    int count = 100000000;
    long time = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      service.testPerformance();
    }
    time = System.currentTimeMillis() - time;
    logger.debug("{} {} ms", label, time);
  }

  private static void testWrapper(TestRetryServiceWrapper serviceWrapper, String label)
      throws Exception {
    serviceWrapper.testPerformance(); // warm up
    int count = 100000000;
    long time = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      serviceWrapper.testPerformance();
    }
    time = System.currentTimeMillis() - time;
    logger.debug("{} {} ms", label, time);
  }

  private static TestRetryService createJavassistBytecodeDynamicProxy(TestRetryService delegate)
      throws Exception {
    ClassPool mpool = new ClassPool(true);
    CtClass mctc = mpool.makeClass(TestRetryService.class.getName() + "JavassistProxy");
    mctc.addInterface(mpool.get(TestRetryService.class.getName()));
    mctc.addConstructor(CtNewConstructor.defaultConstructor(mctc));
    mctc.addField(CtField.make("public " + TestRetryService.class.getName() + " delegate;", mctc));
    mctc.addMethod(CtNewMethod
        .make("public int testPerformance() { return delegate.testPerformance(); }", mctc));
    Class<?> pc = mctc.toClass();
    TestRetryService bytecodeProxy = (TestRetryService) pc.newInstance();
    Field filed = bytecodeProxy.getClass().getField("delegate");
    filed.set(bytecodeProxy, delegate);
    return bytecodeProxy;
  }

  private static TestRetryService createAsmBytecodeDynamicProxy(TestRetryService delegate)
      throws Exception {
    ClassWriter classWriter = new ClassWriter(
        ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
    String className = TestRetryService.class.getName() + "AsmProxy";
    String classPath = className.replace('.', '/');
    String interfacePath = TestRetryService.class.getName().replace('.', '/');
    classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, classPath, null, "java/lang/Object",
        new String[]{interfacePath});

    MethodVisitor initVisitor = classWriter
        .visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    initVisitor.visitCode();
    initVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    initVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
    initVisitor.visitInsn(Opcodes.RETURN);
    initVisitor.visitMaxs(0, 0);
    initVisitor.visitEnd();

    FieldVisitor fieldVisitor = classWriter
        .visitField(Opcodes.ACC_PUBLIC, "delegate", "L" + interfacePath + ";",
            null, null);
    fieldVisitor.visitEnd();

    MethodVisitor methodVisitor = classWriter
        .visitMethod(Opcodes.ACC_PUBLIC, "count", "()I", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor
        .visitFieldInsn(Opcodes.GETFIELD, classPath, "delegate", "L" + interfacePath + ";");
    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, interfacePath, "count", "()I");
    methodVisitor.visitInsn(Opcodes.IRETURN);
    methodVisitor.visitMaxs(0, 0);
    methodVisitor.visitEnd();

    classWriter.visitEnd();
    byte[] code = classWriter.toByteArray();
    TestRetryService bytecodeProxy = (TestRetryService) new ByteArrayClassLoader()
        .getClass(className, code)
        .newInstance();
    Field filed = bytecodeProxy.getClass().getField("delegate");
    filed.set(bytecodeProxy, delegate);
    return bytecodeProxy;
  }

  @Test
  public void testProxyPerformance() throws Exception {
    try {
      final TestRetryService delegate = new TestRetryServiceImpl();

      logger.debug("======== Test proxy class create time ========");

      long time = System.currentTimeMillis();
      TestRetryService noProxy = new TestRetryServiceImpl();
      time = System.currentTimeMillis() - time;
      logger.debug("Create NO Proxy: {} ms", time);

      time = System.currentTimeMillis();
      GenericProxyFactory<TestRetryService> clientProxyFactory = new GenericProxyFactory<>(
          TestRetryService.class, delegate);
      TestRetryService jdkProxy = clientProxyFactory.createJdkDynamicProxy();
      time = System.currentTimeMillis() - time;
      logger.debug("Create JDK Proxy: {} ms", time);

      time = System.currentTimeMillis();
      TestRetryService cglibProxy = clientProxyFactory.createCglibDynamicProxy();
      time = System.currentTimeMillis() - time;
      logger.debug("Create CGLIB Proxy: {} ms", time);

      time = System.currentTimeMillis();
      Class<TProtocol>[] types = new Class[]{};
      Object[] intArgs = new Object[]{};
      TestRetryService javassistProxy = clientProxyFactory
          .createJavassistDynamicProxy(types, intArgs);
      time = System.currentTimeMillis() - time;
      logger.debug("Create JAVAASSIST Proxy: {} ms", time);

      time = System.currentTimeMillis();
      TestRetryService javassistBytecodeProxy = createJavassistBytecodeDynamicProxy(delegate);
      time = System.currentTimeMillis() - time;
      logger.debug("Create JAVAASSIST Bytecode Proxy: {} ms", time);

      // we only consider the higher level of dynamic proxy. ASM is too hard to use.
      time = System.currentTimeMillis();
      TestRetryService asmBytecodeProxy = createAsmBytecodeDynamicProxy(delegate);
      time = System.currentTimeMillis() - time;
      logger.debug("Create ASM Proxy: {} ms", time);

      logger.debug("");
      logger.debug("========Test proxy class run time========");

      for (int i = 0; i < 10; i++) {
        logger.debug("-------round {}---------", i);
        testDelegate(noProxy, "Run NO Proxy: ");
        // testDelegate(jdkProxy, "Run JDK Proxy: ");
        testDelegate(cglibProxy, "Run CGLIB Proxy: ");
        testDelegate(javassistProxy, "Run JAVAASSIST Proxy: ");
        testDelegate(javassistBytecodeProxy, "Run JAVAASSIST Bytecode Proxy: ");

        // we only consider the higher level of dynamic proxy. ASM is too hard to use.
        // testDelegate(asmBytecodeProxy, "Run ASM Bytecode Proxy: ");
      }

      TestRetryServiceWrapper noProxyWrapper = new TestRetryServiceWrapper(noProxy);
      TestRetryServiceWrapper cglibProxyWrapper = new TestRetryServiceWrapper(cglibProxy);
      TestRetryServiceWrapper javassistProxyWrapper = new TestRetryServiceWrapper(javassistProxy);
      TestRetryServiceWrapper javassistBytecodeProxyWrapper = new TestRetryServiceWrapper(
          javassistBytecodeProxy);

      logger.debug("");
      logger.debug("========Test proxy wrapper class run time========");
      for (int i = 0; i < 10; i++) {
        logger.debug("-------round {}---------", i);
        testWrapper(noProxyWrapper, "Run NO Proxy: ");
        testWrapper(cglibProxyWrapper, "Run CGLIB Proxy: ");
        testWrapper(javassistProxyWrapper, "Run JAVAASSIST Proxy: ");
        testWrapper(javassistBytecodeProxyWrapper, "Run JAVAASSIST Bytecode Proxy: ");

        // we only consider the higher level of dynamic proxy. ASM is too hard to use.
        // testWrapper(asmBytecodeProxyWrapper, "Run ASM Bytecode Proxy: ");
      }
    } catch (Throwable e) {
      logger.error("Caught an exception", e);
      Assert.fail();
    }
  }

  @Test
  public void testJavassistMethodName() {
    boolean find = false;
    ClassPool mpool = new ClassPool(true);
    ClassPool pool = ClassPool.getDefault();
    CtClass cc = null;
    try {
      String classPath = "/py/common/client";
      pool.insertClassPath(classPath);
      cc = pool.get("py.common.client.DynamicProxyPerformanceTester");
      CtMethod[] methods = cc.getMethods();
      for (CtMethod method : methods) {
        logger.debug("method name : {} , \t\t\t method :{}", method.getName(), method);
        if (method.getName().equals("testJavassistMethodName")) {
          find = true;
        }
      }
    } catch (Exception e) {
      logger.error("Caught an exception", e);
    }
    Assert.assertTrue(find);
  }

  @Test
  public void testWriteField() throws Exception {
    try {
      ClassPool pool = ClassPool.getDefault();
      pool.importPackage("java.awt");
      pool.importPackage("org.slf4j");
      CtClass cc = pool.makeClass("Test");
      cc.stopPruning(true);
      if (cc.isFrozen()) {
        cc.defrost();
      }

      CtField f = CtField.make("public Point p;", cc);

      cc.addField(f);
      // javassist can't handle private static final field.
      UUID randomDirectory = UUID.randomUUID();
      String path = "/tmp/" + randomDirectory.toString();
      cc.writeFile(path);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      Assert.fail();
    }
  }

  @Test
  public void testWriteFieldTwice() throws Exception {
    try {
      ClassPool pool = ClassPool.getDefault();
      pool.importPackage("java.awt");
      pool.importPackage("org.slf4j");
      CtClass cc = pool.makeClass("Test1");
      cc.stopPruning(true);
      if (cc.isFrozen()) {
        cc.defrost();
      }

      CtField f = CtField.make("public Point p;", cc);

      cc.addField(f);
      UUID randomDirectory = UUID.randomUUID();
      String path = "/tmp/" + randomDirectory.toString();
      cc.writeFile(path);

      cc.stopPruning(true);
      if (cc.isFrozen()) {
        cc.defrost();
      }
      CtField f2 = CtField.make("public Point p1;", cc);
      cc.addField(f2);
      cc.writeFile(path);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      Assert.fail();
    }
  }

  @Test
  public void testReflection() {
    ClassPool mpool = new ClassPool(true);
    final long counter = 1000000000;

    int round = 0;
    while (round++ < 10) {
      logger.debug("\n\n---------------Round {}---------------", round);
      long i = 0;
      long time = System.currentTimeMillis();
      while (i++ < counter) {
        if (mpool instanceof ClassPool) {
          // do nothing
        }
      }
      time = System.currentTimeMillis() - time;
      logger.debug("instanceof : {} ms, invoke once cost: {} ms", time, (double) time / counter);

      i = 0;
      time = System.currentTimeMillis();
      while (i++ < counter) {
        String className = mpool.getClass().getName();
        if (className.equals("mPoolasdfasdfasdfasdfasdf")) {
          // do nothing
        }
      }
      time = System.currentTimeMillis() - time;
      logger.debug("class name string compare : {} ms,invoke once cost: {} ms", time,
          (double) time / counter);
    }
  }

  private static class ByteArrayClassLoader extends ClassLoader {
    public ByteArrayClassLoader() {
      super(ByteArrayClassLoader.class.getClassLoader());
    }

    public synchronized Class<?> getClass(String name, byte[] code) {
      if (name == null) {
        throw new IllegalArgumentException("");
      }
      return defineClass(name, code, 0, code.length);
    }

  }
}
