# Fixed hosts YARN scheduler

Possible implementation variants:

* Extend an existing scheduler (Capacity, Fifo or FairScheduler).
* Extend AbstractYarnScheduler or implement ResourceScheduler.

## SimpleFixedHostsScheduler (based on CapacityScheduler)
Howto use:

1. Compile package as usual and place it in yarn classpath (e.g. `hadoop-2.7.2/share/hadoop/yarn`)
2. Configure YARN scheduler in `etc/hadoop/yarn-site.xml`:
   ```xml
   <property>
        <name>yarn.resourcemanager.scheduler.class</name>
        <value>de.tuberlin.cit.yarn.scheduler.fixedhosts.SimpleFixedHostsScheduler</value>
    </property>
   ```
3. Deploy app with fixed hosts as queue suffix: ```my-queue.fixed-hosts.host-01,host-02...```
