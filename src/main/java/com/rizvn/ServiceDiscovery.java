package com.rizvn;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author fujasuddinr
 */
public class ServiceDiscovery
{
  String insertQuery          = "INSERT INTO SERV_DISC(NAME, VAL, TYPE, LAST_UPDATED) VALUES (?, ?, ?, ?)";
  String updateQuery          = "UPDATE SERV_DISC SET VAL=?, TYPE=?, LAST_UPDATED=? WHERE NAME =?";
  String countServiceByName   = "SELECT COUNT(*) FROM SERV_DISC WHERE NAME =?";
  String serviceByName        = "SELECT NAME, VAL, TYPE FROM SERV_DISC WHERE name = ? AND LAST_UPDATED > ?";
  String servicesByType       = "SELECT NAME, VAL, TYPE FROM SERV_DISC WHERE type = ? AND LAST_UPDATED > ?";
  String removeService        = "DELETE FROM SERV_DISC WHERE NAME = ?";

  DataSource mDataSource;
  Service mThisService;
  Integer updateInterval = 10;

  ScheduledExecutorService mExecuter = Executors.newScheduledThreadPool(1);

  public ServiceDiscovery(String name, String value, String type, Integer updateInterval)
  {
    mThisService = new Service();
    mThisService.name = name;
    mThisService.value = value;
    mThisService.type = type;

    this.updateInterval = updateInterval;
  }

  public ServiceDiscovery(String name, String value, String type)
  {
    mThisService = new Service();
    mThisService.name = name;
    mThisService.value = value;
    mThisService.type = type;
  }

  public void start()
  {
    mExecuter.scheduleWithFixedDelay(this::register, 0,  updateInterval, TimeUnit.SECONDS);
  }

  public void register()
  {
    if(countServiceByName(mThisService.getName()) < 1)
    {
      insertService();
    }
    else
    {
      updateService();
    }
  }

  public void removeService()
  {
    try(Connection conn = mDataSource.getConnection())
    {
      PreparedStatement stmt = conn.prepareStatement(removeService);
      stmt.setString(1, mThisService.name);
      stmt.execute();
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  public void insertService()
  {
    try(Connection conn = mDataSource.getConnection())
    {
      PreparedStatement stmt = conn.prepareStatement(insertQuery);
      stmt.setString(1, mThisService.name);
      stmt.setString(2, mThisService.value);
      stmt.setString(3, mThisService.type);
      stmt.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
      stmt.execute();
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  public void updateService()
  {
    try(Connection conn = mDataSource.getConnection())
    {
      PreparedStatement stmt = conn.prepareStatement(updateQuery);
      stmt.setString(1, mThisService.value);
      stmt.setString(2, mThisService.type);
      stmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
      stmt.setString(4, mThisService.name);
      stmt.execute();
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }



  public Long countServiceByName(String aServiceName)
  {
    try(Connection conn = mDataSource.getConnection())
    {
      PreparedStatement stmt = conn.prepareStatement(countServiceByName);
      stmt.setString(1, aServiceName);

      ResultSet resultSet = stmt.executeQuery();
      if (resultSet.first())
      {
        return resultSet.getLong(1);
      }
      else
      {
        return -1L;
      }
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Find services by type, uses update interval to not pick up
   * services that have not been refershed on the heart beat interval
   * @param serviceType Type of service
   * @return List of services found
   */
  public List<Service> findByType(String serviceType)
  {
    List<Service> services = new ArrayList<>();

    try(Connection conn = mDataSource.getConnection())
    {
      PreparedStatement stmt = conn.prepareStatement(servicesByType);
      stmt.setString(1, serviceType);
      stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now().minusSeconds(updateInterval)));

      ResultSet resultSet = stmt.executeQuery();

      while (resultSet.next())
      {
        Service service = new Service();
        service.name = resultSet.getString(1);
        service.value = resultSet.getString(2);
        service.type = resultSet.getString(3);
        services.add(service);
      }

      return services;
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  public Service findByName(String serviceName)
  {
    try(Connection conn = mDataSource.getConnection())
    {
      PreparedStatement stmt = conn.prepareStatement(serviceByName);
      stmt.setString(1, serviceName);
      stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now().minusSeconds(updateInterval)));
      ResultSet resultSet = stmt.executeQuery();

      if(resultSet.next())
      {
        Service service = new Service();
        service.name = resultSet.getString(1);
        service.value = resultSet.getString(2);
        service.type = resultSet.getString(3);
        return service;
      }
      else
      {
        return null;
      }
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }

  }

  public static class Service
  {
    String name;
    String value;
    String type;

    public String getName()
    {
      return name;
    }

    public void setName(String aName)
    {
      name = aName;
    }

    public String getValue()
    {
      return value;
    }

    public void setValue(String aValue)
    {
      value = aValue;
    }

    public String getType()
    {
      return type;
    }

    public void setType(String aType)
    {
      type = aType;
    }
  }

  public DataSource getDataSource() {
    return mDataSource;
  }

  public void setDataSource(DataSource aDataSource) {
    mDataSource = aDataSource;
  }
}