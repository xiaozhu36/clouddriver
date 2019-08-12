/*
 * Copyright 2019 Alibaba Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.clouddriver.alicloud.provider.agent;

import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.APPLICATIONS;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.CLUSTERS;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.INSTANCES;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.LAUNCH_CONFIGS;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.LOAD_BALANCERS;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.SERVER_GROUPS;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.aliyuncs.ess.model.v20140828.DescribeScalingConfigurationsResponse;
import com.aliyuncs.ess.model.v20140828.DescribeScalingConfigurationsResponse.ScalingConfiguration;
import com.aliyuncs.ess.model.v20140828.DescribeScalingGroupsResponse;
import com.aliyuncs.ess.model.v20140828.DescribeScalingGroupsResponse.ScalingGroup;
import com.aliyuncs.ess.model.v20140828.DescribeScalingInstancesResponse;
import com.aliyuncs.ess.model.v20140828.DescribeScalingInstancesResponse.ScalingInstance;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.slb.model.v20140515.DescribeLoadBalancerAttributeResponse;
import com.netflix.frigga.Names;
import com.netflix.spinnaker.cats.agent.CacheResult;
import com.netflix.spinnaker.cats.cache.CacheData;
import com.netflix.spinnaker.clouddriver.alicloud.cache.Keys;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class AliCloudClusterCachingAgentTest extends CommonCachingAgentTest {

  private final String SCALINGGROUPID = "sgid";
  private final String SCALINGGROUPNAME = "spinnaker-ess-test";
  private final String SCALINGCONFIGID = "scid";
  private final String LOADBALANCERID = "lbid";
  private final String LOADBALANCERNAME = "lbname";
  private final String INSTANCEID = "inid";

  @Before
  public void testBefore() throws ClientException {
    when(client.getAcsResponse(any()))
        .thenAnswer(new ScalingGroupsAnswer())
        .thenAnswer(new ScalingConfigurationsAnswer())
        .thenAnswer(new LoadBalancerAttributeAnswer())
        .thenAnswer(new ScalingInstancesAnswer());
  }

  @Test
  public void testLoadData() {

    Names name = Names.parseName(SCALINGGROUPNAME);
    AliCloudClusterCachingAgent agent =
        new AliCloudClusterCachingAgent(account, REGION, objectMapper, client);
    CacheResult result = agent.loadData(providerCache);

    Map<String, Collection<CacheData>> cacheResults = result.getCacheResults();
    Collection<CacheData> applications = cacheResults.get(APPLICATIONS.ns);
    Collection<CacheData> clusters = cacheResults.get(CLUSTERS.ns);
    Collection<CacheData> serverGroups = cacheResults.get(SERVER_GROUPS.ns);
    Collection<CacheData> instances = cacheResults.get(INSTANCES.ns);
    Collection<CacheData> loadBalancers = cacheResults.get(LOAD_BALANCERS.ns);
    Collection<CacheData> launchConfigs = cacheResults.get(LAUNCH_CONFIGS.ns);

    String applicationKey = Keys.getApplicationKey(name.getApp());
    String clusterKey = Keys.getClusterKey(name.getCluster(), name.getApp(), ACCOUNT);
    String serverGroupKey = Keys.getServerGroupKey(SCALINGGROUPNAME, ACCOUNT, REGION);
    String instanceKey = Keys.getInstanceKey(INSTANCEID, ACCOUNT, REGION);
    String loadBalancerKey = Keys.getLoadBalancerKey(LOADBALANCERNAME, ACCOUNT, REGION, null);
    String launchConfigKey = Keys.getLaunchConfigKey(SCALINGGROUPNAME, ACCOUNT, REGION);

    assertTrue(applications.size() == 1);
    assertTrue(applicationKey.equals(applications.iterator().next().getId()));

    assertTrue(clusters.size() == 1);
    assertTrue(clusterKey.equals(clusters.iterator().next().getId()));

    assertTrue(serverGroups.size() == 1);
    assertTrue(serverGroupKey.equals(serverGroups.iterator().next().getId()));

    assertTrue(instances.size() == 1);
    assertTrue(instanceKey.equals(instances.iterator().next().getId()));

    assertTrue(loadBalancers.size() == 1);
    assertTrue(loadBalancerKey.equals(loadBalancers.iterator().next().getId()));

    assertTrue(launchConfigs.size() == 1);
    assertTrue(launchConfigKey.equals(launchConfigs.iterator().next().getId()));
  }

  private class ScalingGroupsAnswer implements Answer<DescribeScalingGroupsResponse> {
    @Override
    public DescribeScalingGroupsResponse answer(InvocationOnMock invocation) throws Throwable {
      DescribeScalingGroupsResponse response = new DescribeScalingGroupsResponse();
      List<ScalingGroup> scalingGroups = new ArrayList<>();
      ScalingGroup scalingGroup = new ScalingGroup();
      scalingGroup.setMinSize(3);
      scalingGroup.setMaxSize(10);
      scalingGroup.setScalingGroupId(SCALINGGROUPID);
      scalingGroup.setActiveScalingConfigurationId(SCALINGCONFIGID);
      scalingGroup.setScalingGroupName(SCALINGGROUPNAME);

      List<String> loadBalancerIds = new ArrayList<>();
      loadBalancerIds.add(LOADBALANCERID);
      scalingGroup.setLoadBalancerIds(loadBalancerIds);

      scalingGroups.add(scalingGroup);
      response.setScalingGroups(scalingGroups);
      return response;
    }
  }

  private class ScalingConfigurationsAnswer
      implements Answer<DescribeScalingConfigurationsResponse> {
    @Override
    public DescribeScalingConfigurationsResponse answer(InvocationOnMock invocation)
        throws Throwable {
      DescribeScalingConfigurationsResponse response = new DescribeScalingConfigurationsResponse();
      List<ScalingConfiguration> scalingConfigurations = new ArrayList<>();
      ScalingConfiguration scalingConfiguration = new ScalingConfiguration();
      scalingConfiguration.setScalingConfigurationId(SCALINGCONFIGID);

      scalingConfigurations.add(scalingConfiguration);
      response.setScalingConfigurations(scalingConfigurations);
      return response;
    }
  }

  private class LoadBalancerAttributeAnswer
      implements Answer<DescribeLoadBalancerAttributeResponse> {
    @Override
    public DescribeLoadBalancerAttributeResponse answer(InvocationOnMock invocation)
        throws Throwable {
      DescribeLoadBalancerAttributeResponse response = new DescribeLoadBalancerAttributeResponse();
      response.setLoadBalancerId(LOADBALANCERID);
      response.setLoadBalancerName(LOADBALANCERNAME);
      return response;
    }
  }

  private class ScalingInstancesAnswer implements Answer<DescribeScalingInstancesResponse> {
    @Override
    public DescribeScalingInstancesResponse answer(InvocationOnMock invocation) throws Throwable {
      DescribeScalingInstancesResponse response = new DescribeScalingInstancesResponse();
      List<ScalingInstance> scalingInstances = new ArrayList<>();
      ScalingInstance instance = new ScalingInstance();
      instance.setInstanceId(INSTANCEID);

      scalingInstances.add(instance);
      response.setScalingInstances(scalingInstances);
      return response;
    }
  }
}
