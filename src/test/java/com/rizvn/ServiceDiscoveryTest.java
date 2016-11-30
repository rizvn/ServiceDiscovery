package com.rizvn;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Created by Riz
 */
public class ServiceDiscoveryTest {

  ServiceDiscovery serviceDiscovery;

  @Before
  public void setUp()
  {
    MysqlDataSource ds = new MysqlDataSource();
    ds.setUser("root");
    ds.setPassword("riz123");
    ds.setUrl("jdbc:mysql://localhost:3306/testdb");

    serviceDiscovery = new ServiceDiscovery("ABC", "123", "TYPEA", 10);
    serviceDiscovery.setDataSource(ds);
  }

  @Test
  public void remove()
  {
    serviceDiscovery.removeService();
  }

  @Test
  public void insert()
  {
    serviceDiscovery.removeService();
    serviceDiscovery.insertService();
  }

  @Test
  public void updateService()
  {
    serviceDiscovery.removeService();
    serviceDiscovery.insertService();
    serviceDiscovery.updateService();
  }

  @Test
  public void register()
  {
    serviceDiscovery.register();
  }

  @Test
  public void findByName()
  {
    serviceDiscovery.register();
    ServiceDiscovery.Service service = serviceDiscovery.findByName("ABC");
    Assert.assertEquals("ABC", service.getName());
  }

  @Test
  public void findByNameAfter10Secs() throws Exception
  {
    serviceDiscovery.register();
    Thread.sleep(11000);
    ServiceDiscovery.Service service = serviceDiscovery.findByName("ABC");
    Assert.assertNull(service);
  }

  @Test
  public void findByType()
  {
    serviceDiscovery.register();
    List<ServiceDiscovery.Service> services = serviceDiscovery.findByType("TYPEA");
    Assert.assertEquals(1, services.size());
  }

  @Test
  public void findByTypeNotVisibleAfter10Secs() throws Exception
  {
    serviceDiscovery.register();
    Thread.sleep(11000);
    List<ServiceDiscovery.Service> services = serviceDiscovery.findByType("TYPEA");
    Assert.assertEquals(0, services.size());
  }

  @Test
  public void testScheduledUpdate() throws Exception
  {
    ServiceDiscovery.Service service;
    serviceDiscovery.start();

    Thread.sleep(11000);
    service = serviceDiscovery.findByName("ABC");
    Assert.assertNotNull(service);

    Thread.sleep(11000);
    service = serviceDiscovery.findByName("ABC");
    Assert.assertNotNull(service);

  }




}