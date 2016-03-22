package de.tuberlin.cit.yarn.scheduler.fixedhosts;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * A capacity scheduler with host whitelisting.
 *
 * Deploy app with a fixed host list as queue suffix:
 * E.g. queue ,,my-queue.fixed-hosts.host-01,host-02'' will be mapped to my-queue
 * with fixed hosts host-01 and host-02.
 */
public class SimpleFixedHostsScheduler extends CapacityScheduler {
	public static final Log LOG = LogFactory.getLog(SimpleFixedHostsScheduler.class);
	public static final String QUEUE_ADDON = ".fixed-hosts.";
	private final HashMap<ApplicationId, String> fixedHosts = new HashMap<ApplicationId, String>();

	public static class FixedHostSchedulerApp extends FiCaSchedulerApp {
		private final Set<String> fixedHosts;

		public FixedHostSchedulerApp(ApplicationAttemptId applicationAttemptId,
									 String fixedHosts,
								     String user,
									 Queue queue,
									 ActiveUsersManager activeUsersManager,
								     RMContext rmContext) {
			super(applicationAttemptId, user, queue, activeUsersManager, rmContext);

			if (fixedHosts != null) {
				LOG.info("Initializing new Application Attempt " + applicationAttemptId
						+ " with fixed hosts list: " + fixedHosts);

				this.fixedHosts = new HashSet<String>();
				for (String host : fixedHosts.split(",")) {
					this.fixedHosts.add(host);
				}
			} else {
				this.fixedHosts = null;
			}
		}

		@Override
		public synchronized Allocation getAllocation(ResourceCalculator rc, Resource clusterResource, Resource minimumAllocation) {
			// Override me if required.
			// LOG.info("Got getAllocation call.");
			return super.getAllocation(rc, clusterResource, minimumAllocation);
		}

		@Override
		public boolean isBlacklisted(String resourceName) {
			if (this.fixedHosts != null && !resourceName.equals("/default-rack")) {
				LOG.warn("Got isBlacklisted on " + resourceName + " with fixed hosts list.");
				return this.fixedHosts.contains(resourceName);
			} else {
				return super.isBlacklisted(resourceName);
			}
		}
	}

	/**
	 * Override some events.
	 */
	@Override
	public void handle(SchedulerEvent event) {
		if (event.getType() == SchedulerEventType.APP_ADDED) {
			AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;

			if (appAddedEvent.getQueue().contains(QUEUE_ADDON)) {
				String queue = appAddedEvent.getQueue();
				String hosts = queue.substring(queue.indexOf(QUEUE_ADDON) + QUEUE_ADDON.length());
				String newQueue = queue.substring(0, queue.indexOf(QUEUE_ADDON));
				this.fixedHosts.put(appAddedEvent.getApplicationId(), hosts);

				LOG.info("Replacing special fixed hosts queue name " + appAddedEvent.getQueue()
						+ " with " + newQueue + ".");
				appAddedEvent =	new AppAddedSchedulerEvent(
									appAddedEvent.getApplicationId(),
									newQueue,
									appAddedEvent.getUser(),
									appAddedEvent.getIsAppRecovering(),
									appAddedEvent.getReservationID());
			}

			super.handle(appAddedEvent);

		} else if (event.getType() == SchedulerEventType.APP_ATTEMPT_ADDED) {
			AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
					(AppAttemptAddedSchedulerEvent) event;
			addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
					appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
					appAttemptAddedEvent.getIsAttemptRecovering());

		} else if (event.getType() == SchedulerEventType.APP_REMOVED) {
			AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
			fixedHosts.remove(appRemovedEvent.getApplicationID());
			super.handle(appRemovedEvent);

		} else {
			super.handle(event);
		}
	}

	/**
	 * Copy & paste from capacity scheduler with FixedHostSchedulerApp instead of FiCaSchedulerApp.
	 */
	private synchronized void addApplicationAttempt(
			ApplicationAttemptId applicationAttemptId,
			boolean transferStateFromPreviousAttempt,
			boolean isAttemptRecovering) {
		SchedulerApplication<FiCaSchedulerApp> application =
				applications.get(applicationAttemptId.getApplicationId());
		if (application == null) {
			LOG.warn("Application " + applicationAttemptId.getApplicationId() +
					" cannot be found in scheduler.");
			return;
		}
		CSQueue queue = (CSQueue) application.getQueue();

		FiCaSchedulerApp attempt =
				new FixedHostSchedulerApp(applicationAttemptId,
						this.fixedHosts.get(applicationAttemptId.getApplicationId()),
						application.getUser(),	queue, queue.getActiveUsersManager(), rmContext);
		if (transferStateFromPreviousAttempt) {
			attempt.transferStateFromPreviousAttempt(application
					.getCurrentAppAttempt());
		}
		application.setCurrentAppAttempt(attempt);

		queue.submitApplicationAttempt(attempt, application.getUser());
		LOG.info("Added Application Attempt " + applicationAttemptId
				+ " to scheduler from user " + application.getUser() + " in queue "
				+ queue.getQueueName());
		if (isAttemptRecovering) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(applicationAttemptId
						+ " is recovering. Skipping notifying ATTEMPT_ADDED");
			}
		} else {
			rmContext.getDispatcher().getEventHandler().handle(
					new RMAppAttemptEvent(applicationAttemptId,
							RMAppAttemptEventType.ATTEMPT_ADDED));
		}
	}

}
