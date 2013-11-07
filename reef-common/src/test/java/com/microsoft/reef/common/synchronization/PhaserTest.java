package com.microsoft.reef.common.synchronization;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.reef.common.synchronization.Phaser;
import com.microsoft.reef.common.synchronization.Phaser.Master;
import com.microsoft.reef.common.synchronization.Phaser.NumParticipants;
import com.microsoft.reef.common.synchronization.Phaser.ParticipantBuilder;
import com.microsoft.reef.common.synchronization.Phaser.Participants;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.wake.remote.RemoteConfiguration;
import com.microsoft.wake.remote.RemoteConfiguration.ManagerName;


public class PhaserTest {

  @Rule public TestName name = new TestName();

  @Test
  public void testPhaser() throws Exception {
    System.out.println(name.getMethodName());
    
    
    Injector inj1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 1");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "1111");
      inj1 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm1 = inj1.getInstance(RemoteManager.class);
    String id1 = rm1.getMyIdentifier();
    
    Injector inj2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 2");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "1112");
      inj2 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm2 = inj2.getInstance(RemoteManager.class);
    String id2 = rm2.getMyIdentifier();
   
    Injector pi1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Participants.class, ParticipantBuilder.newBuilder()
          .add(id1)
          .add(id2)
          .build());
      cb.bindNamedParameter(Master.class, id1);
      cb.bindNamedParameter(NumParticipants.class, "2");
      pi1 = inj1.createChildInjector(cb.build());
    }
    
    Injector pi2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Master.class, id1);
      pi2 = inj2.createChildInjector(cb.build());
     // pi2.bindVolatileInstance(RemoteManager.class, rm2);
    }

    final Phaser dut2 = pi2.getInstance(Phaser.class);
    final Phaser dut1 = pi1.getInstance(Phaser.class);  
    
    ExecutorService e = Executors.newCachedThreadPool();
    e.submit(new Runnable() {
      
      @Override
      public void run() {
        try {
          dut1.signal();
          System.out.println("1 signaled");
          dut1.waitAll();
          System.out.println("1 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("1 exiting");
        }
      }
    });
    
    e.submit(new Runnable() {

      @Override
      public void run() {
        try {
          dut2.signal();
          System.out.println("2 signaled");

          dut2.waitAll();
          System.out.println("2 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("2 exiting");
        }
      }
    });
    
    e.shutdown();
    Assert.assertTrue(e.awaitTermination(3, TimeUnit.SECONDS));
    
    rm1.close();
    rm2.close();
  }
  
  @Test
  public void testDelayedRegistration() throws Exception {
    System.out.println(name.getMethodName());
    
    
    Injector inj1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 1");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "1111");
      inj1 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm1 = inj1.getInstance(RemoteManager.class);
    String id1 = rm1.getMyIdentifier();
    
    Injector inj2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 2");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "1112");
      inj2 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm2 = inj2.getInstance(RemoteManager.class);
   
    Injector pi1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      /* 
       * Omitting the Participants field to allow delayed registration to trigger 
       */
      cb.bindNamedParameter(Master.class, id1);
      cb.bindNamedParameter(NumParticipants.class, "2");
      pi1 = inj1.createChildInjector(cb.build());
    }
    
    Injector pi2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Master.class, id1);
      pi2 = inj2.createChildInjector(cb.build());
    }

    final Phaser dut2 = pi2.getInstance(Phaser.class);
    final Phaser dut1 = pi1.getInstance(Phaser.class);  
    
    ExecutorService e = Executors.newCachedThreadPool();
    e.submit(new Runnable() {
      
      @Override
      public void run() {
        try {
          dut1.signal();
          System.out.println("1 signaled");
          dut1.waitAll();
          System.out.println("1 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("1 exiting");
        }
      }
    });
    
    e.submit(new Runnable() {

      @Override
      public void run() {
        try {
          dut2.signal();
          System.out.println("2 signaled");

          dut2.waitAll();
          System.out.println("2 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("2 exiting");
        }
      }
    });
    
    e.shutdown();
    Assert.assertTrue(e.awaitTermination(3, TimeUnit.SECONDS));
    
    rm1.close();
    rm2.close();
  }
  
  @Test
  public void testParentToMany() throws Exception {
    System.out.println(name.getMethodName());
    
    int numChildren = 13;
    
    Injector inj1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 1");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "1111");
      inj1 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm1 = inj1.getInstance(RemoteManager.class);
    String id1 = rm1.getMyIdentifier();
    
    Injector[] childInjectors = new Injector[numChildren];
    RemoteManager[] childRMs = new RemoteManager[numChildren];
    for (int t=0; t<numChildren; t++)
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest " + (t+2));
      cb.bindNamedParameter(RemoteConfiguration.Port.class, Integer.toString(1112+t));
      childInjectors[t] = Tang.Factory.getTang().newInjector(cb.build());
      childRMs[t] = childInjectors[t].getInstance(RemoteManager.class);
    }
   
    Injector pi1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      /* 
       * Omitting the Participants field to allow delayed registration to trigger 
       */
      cb.bindNamedParameter(Master.class, id1);
      cb.bindNamedParameter(NumParticipants.class, Integer.toString(numChildren));
      pi1 = inj1.createChildInjector(cb.build());
    }
    
    Injector[] phaserInjectors = new Injector[numChildren];
    for (int t=0; t<numChildren; t++)
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Master.class, id1);
      phaserInjectors[t] = childInjectors[t].createChildInjector(cb.build());
    }

    final Phaser dut1 = pi1.getInstance(Phaser.class);  
    final Phaser[] childDuts = new Phaser[numChildren];
    for (int t=0; t<numChildren; t++) {
      childDuts[t] = phaserInjectors[t].getInstance(Phaser.class);
    }
    
    ExecutorService e = Executors.newCachedThreadPool();
    e.submit(new Runnable() {
      
      @Override
      public void run() {
        try {
          System.out.println("master waiting");
          dut1.waitAll();
          System.out.println("master finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("master exiting");
        }
      }
    });
    
    for (int t=0; t<numChildren; t++) {
      final int tt = t;
      e.submit(new Runnable() {

        @Override
        public void run() {
          try {
            childDuts[tt].signal();
            System.out.println(tt+" signaled");

            childDuts[tt].waitAll();
            System.out.println(tt+" finished waiting");
          } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
          } finally {
            System.out.println(tt+" exiting");
          }
        }
      });
    }
    
    e.shutdown();
    Assert.assertTrue(e.awaitTermination(3, TimeUnit.SECONDS));
    
    rm1.close();
    for (int t=0; t<numChildren; t++) {
      childRMs[t].close();
    }
  }

}

