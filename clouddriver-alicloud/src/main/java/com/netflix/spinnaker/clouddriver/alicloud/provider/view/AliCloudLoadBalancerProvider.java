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
package com.netflix.spinnaker.clouddriver.alicloud.provider.view;

import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.APPLICATIONS;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.INSTANCES;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.LOAD_BALANCERS;
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.SERVER_GROUPS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.cats.cache.Cache;
import com.netflix.spinnaker.cats.cache.CacheData;
import com.netflix.spinnaker.cats.cache.CacheFilter;
import com.netflix.spinnaker.cats.cache.RelationshipCacheFilter;
import com.netflix.spinnaker.clouddriver.alicloud.AliCloudProvider;
import com.netflix.spinnaker.clouddriver.alicloud.cache.Keys;
import com.netflix.spinnaker.clouddriver.alicloud.model.AliCloudInstance;
import com.netflix.spinnaker.clouddriver.alicloud.model.AliCloudLoadBalancer;
import com.netflix.spinnaker.clouddriver.alicloud.model.AliCloudServerGroup;
import com.netflix.spinnaker.clouddriver.model.HealthState;
import com.netflix.spinnaker.clouddriver.model.LoadBalancerProvider;
import com.netflix.spinnaker.clouddriver.model.LoadBalancerServerGroup;
import com.netflix.spinnaker.clouddriver.model.ServerGroup.Capacity;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AliCloudLoadBalancerProvider implements LoadBalancerProvider<AliCloudLoadBalancer> {

  private final ObjectMapper objectMapper;

  private final Cache cacheView;

  @Autowired
  public AliCloudLoadBalancerProvider(ObjectMapper objectMapper, Cache cacheView) {
    this.objectMapper = objectMapper;
    this.cacheView = cacheView;
  }

  Collection<CacheData> resolveRelationshipData(
      CacheData source, String relationship, CacheFilter cacheFilter) {
    Map<String, Collection<String>> relationships = source.getRelationships();
    Collection<String> keys = relationships.get(relationship);
    if (!keys.isEmpty()) {
      return cacheView.getAll(relationship, keys, null);
    } else {
      return new ArrayList<CacheData>();
    }
  }

  private Collection<CacheData> resolveRelationshipDataForCollection(
      Collection<CacheData> sources, String relationship, CacheFilter cacheFilter) {
    Set<String> relationships = new HashSet<>();
    sources.forEach(
        data -> {
          Collection<String> keys = data.getRelationships().get(relationship);
          if (keys != null) {
            relationships.addAll(keys);
          }
        });
    if (relationships.isEmpty()) {
      return new ArrayList<>();
    } else {
      return cacheView.getAll(relationship, relationships, null);
    }
  }

  private Map<String, AliCloudInstance> translateInstances(Collection<CacheData> instanceData) {
    Map<String, AliCloudInstance> instances =
        instanceData.stream()
            .collect(
                Collectors.toMap(
                    CacheData::getId,
                    data ->
                        new AliCloudInstance(
                            String.valueOf(data.getAttributes().get("instanceId")),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null)));
    return instances;
  }

  private static Map<String, AliCloudServerGroup> translateServerGroups(
      Collection<CacheData> serverGroupData, Map<String, AliCloudInstance> allInstances) {
    Map<String, AliCloudServerGroup> collect =
        serverGroupData.stream()
            .collect(
                Collectors.toMap(CacheData::getId, data -> buildServerGroup(data, allInstances)));
    return collect;
  }

  private static AliCloudServerGroup buildServerGroup(
      CacheData data, Map<String, AliCloudInstance> allInstances) {
    Map<String, Object> attributes = data.getAttributes();

    AliCloudServerGroup serverGroup = new AliCloudServerGroup();

    Set<AliCloudInstance> instances = new HashSet<>();
    Map<String, Collection<String>> relationships = data.getRelationships();
    Collection<String> keys = relationships.get(INSTANCES.ns);
    keys.forEach(
        key -> {
          if (allInstances.containsKey(key)) {
            instances.add(allInstances.get(key));
          }
        });

    serverGroup.setType(AliCloudProvider.ID);
    serverGroup.setName(String.valueOf(attributes.get("name")));
    serverGroup.setCloudProvider(AliCloudProvider.ID);
    serverGroup.setRegion(String.valueOf(attributes.get("region")));

    Map<String, Object> scalingGroup = (Map) attributes.get("scalingGroup");
    serverGroup.setResult(scalingGroup);

    String lifecycleState = (String) scalingGroup.get("lifecycleState");
    if ("Active".equals(lifecycleState)) {
      serverGroup.setDisabled(false);
    } else {
      serverGroup.setDisabled(true);
    }

    serverGroup.setCreationTime(String.valueOf(scalingGroup.get("creationTime")));
    String date = String.valueOf(scalingGroup.get("creationTime"));
    date = date.replace("Z", " UTC");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm Z");
    try {
      Date d = format.parse(date);
      serverGroup.setCreatedTime(d.getTime());
    } catch (ParseException e) {
      e.printStackTrace();
    }
    List<Map> instancesMap = (List<Map>) attributes.get("instances");

    for (Map instance : instancesMap) {
      Object id = instance.get("instanceId");
      if (id != null) {
        String healthStatus = (String) instance.get("healthStatus");
        boolean flag = "Healthy".equals(healthStatus);

        List<Map<String, Object>> health = new ArrayList<>();
        Map<String, Object> m = new HashMap<>();
        m.put("type", "AlibabaCloud");
        m.put("healthClass", "platform");

        m.put(
            "state",
            !"Active".equals(lifecycleState)
                ? HealthState.Down
                : flag ? HealthState.Up : HealthState.Down);
        health.add(m);
        String zone = (String) instance.get("creationType");
        AliCloudInstance i =
            new AliCloudInstance(
                String.valueOf(id),
                null,
                zone,
                null,
                AliCloudProvider.ID,
                (!"Active".equals(lifecycleState)
                    ? HealthState.Down
                    : flag ? HealthState.Up : HealthState.Down),
                health);
        instances.add(i);
      }
    }
    serverGroup.setInstances(instances);

    // build capacity
    Capacity capacity = new Capacity();
    Object maxSize = scalingGroup.get("maxSize");
    Object minSize = scalingGroup.get("minSize");

    capacity.setMax((Integer) maxSize);
    capacity.setMin((Integer) minSize);
    capacity.setDesired(instances.size());
    serverGroup.setCapacity(capacity);

    serverGroup.setResult(data.getAttributes());

    // build image info
    Map<String, Object> scalingConfiguration = (Map) attributes.get("scalingConfiguration");
    serverGroup.setLaunchConfig(scalingConfiguration);
    Map<String, Object> image = new HashMap<>();
    image.put("name", scalingConfiguration.get("imageId"));
    image.put("imageId", scalingConfiguration.get("imageId"));
    Map buildInfo = new HashMap();
    buildInfo.put("imageId", scalingConfiguration.get("imageId"));
    serverGroup.setImage(image);
    serverGroup.setBuildInfo(buildInfo);
    return serverGroup;
  }

  public static AliCloudLoadBalancer buildLoadBalancer(
      CacheData data, Map<String, AliCloudServerGroup> serverGroups, ObjectMapper objectMapper) {
    Map<String, Object> attributes = objectMapper.convertValue(data.getAttributes(), Map.class);
    AliCloudLoadBalancer loadBalancer =
        new AliCloudLoadBalancer(
            String.valueOf(attributes.get("account")),
            String.valueOf(attributes.get("regionIdAlias")),
            String.valueOf(attributes.get("loadBalancerName")),
            String.valueOf(attributes.get("vpcId")),
            String.valueOf(attributes.get("loadBalancerId")));
    Map<String, Collection<String>> relationships = data.getRelationships();
    Collection<String> keys =
        relationships.get(SERVER_GROUPS.ns) != null
            ? relationships.get(SERVER_GROUPS.ns)
            : new ArrayList<>();
    Collection<AliCloudServerGroup> lbServerGroups = new ArrayList<>();
    keys.forEach(
        key -> {
          if (serverGroups.containsKey(key)) {
            lbServerGroups.add(serverGroups.get(key));
          }
        });
    Set<LoadBalancerServerGroup> serverGroupSet = new HashSet<>();

    lbServerGroups.forEach(
        serverGroup -> {
          serverGroupSet.add(
              createLoadBalancerServerGroup(
                  serverGroup, "loadBalancerName", loadBalancer.getName()));
        });
    loadBalancer.setServerGroups(serverGroupSet);
    return loadBalancer;
  }

  private static LoadBalancerServerGroup createLoadBalancerServerGroup(
      AliCloudServerGroup serverGroup, String healthKey, String name) {
    LoadBalancerServerGroup loadBalancerServerGroup = new LoadBalancerServerGroup();
    loadBalancerServerGroup.setName(serverGroup.getName());
    loadBalancerServerGroup.setIsDisabled(true);
    loadBalancerServerGroup.setCloudProvider(serverGroup.getCloudProvider());
    return loadBalancerServerGroup;
  }

  private static Set<AliCloudLoadBalancer> translateLoadBalancers(
      Collection<CacheData> loadBalancerData,
      Map<String, AliCloudServerGroup> serverGroups,
      ObjectMapper objectMapper) {
    Set<AliCloudLoadBalancer> loadBalancers =
        loadBalancerData.stream()
            .map(data -> buildLoadBalancer(data, serverGroups, objectMapper))
            .collect(Collectors.toSet());
    return loadBalancers;
  }

  @Override
  public Set<AliCloudLoadBalancer> getApplicationLoadBalancers(String applicationName) {
    Set<String> loadBalancerKeys = new HashSet<>();

    CacheData application = cacheView.get(APPLICATIONS.ns, Keys.getApplicationKey(applicationName));
    Collection<CacheData> applicationServerGroups = new ArrayList<>();
    if (application != null) {
      applicationServerGroups =
          resolveRelationshipData(
              application,
              SERVER_GROUPS.ns,
              RelationshipCacheFilter.include(INSTANCES.ns, LOAD_BALANCERS.ns));
    }

    Collection<String> allLoadBalancerKeys = cacheView.getIdentifiers(LOAD_BALANCERS.ns);

    applicationServerGroups.forEach(
        serverGroup -> {
          Collection<String> serverGroupLoadBalancers =
              serverGroup.getRelationships().get(LOAD_BALANCERS.ns) != null
                  ? serverGroup.getRelationships().get(LOAD_BALANCERS.ns)
                  : new ArrayList<String>();
          serverGroupLoadBalancers.forEach(
              key -> {
                loadBalancerKeys.add(key);
                String vpcKey = key + ":vpc-";
                List<String> startsWithKeys =
                    allLoadBalancerKeys.stream()
                        .filter(str -> str.startsWith(vpcKey))
                        .collect(Collectors.toList());
                loadBalancerKeys.addAll(startsWithKeys);
              });
        });

    Collection<String> loadBalancerKeyMatches =
        allLoadBalancerKeys.stream()
            .filter(tab -> applicationMatcher(tab, applicationName))
            .collect(Collectors.toList());
    loadBalancerKeys.addAll(loadBalancerKeyMatches);

    Collection<CacheData> loadBalancerData =
        cacheView.getAll(LOAD_BALANCERS.ns, loadBalancerKeys, null);
    Collection<CacheData> allLoadBalancerServerGroups =
        resolveRelationshipDataForCollection(
            loadBalancerData, SERVER_GROUPS.ns, RelationshipCacheFilter.none());
    Collection<CacheData> allLoadBalancerInstances =
        resolveRelationshipDataForCollection(
            allLoadBalancerServerGroups, INSTANCES.ns, RelationshipCacheFilter.none());

    Map<String, AliCloudInstance> loadBalancerInstances =
        translateInstances(allLoadBalancerInstances);
    Map<String, AliCloudServerGroup> loadBalancerServerGroups =
        translateServerGroups(allLoadBalancerServerGroups, loadBalancerInstances);

    return translateLoadBalancers(loadBalancerData, loadBalancerServerGroups, objectMapper);
  }

  @Override
  public List<ResultDetails> byAccountAndRegionAndName(String account, String region, String name) {
    List<ResultDetails> results = new ArrayList<>();
    String searchKey = Keys.getLoadBalancerKey(name, account, region, null) + "*";
    Collection<String> allLoadBalancerKeys =
        cacheView.filterIdentifiers(LOAD_BALANCERS.ns, searchKey);
    Collection<CacheData> loadBalancers =
        cacheView.getAll(LOAD_BALANCERS.ns, allLoadBalancerKeys, null);
    for (CacheData loadBalancer : loadBalancers) {
      ResultDetails resultDetails = new ResultDetails();
      resultDetails.setResults(loadBalancer.getAttributes());
      results.add(resultDetails);
    }

    return results;
  }

  class ResultDetails implements Details {

    Map results;

    public Map getResults() {
      return results;
    }

    public void setResults(Map results) {
      this.results = results;
    }
  }

  @Override
  public String getCloudProvider() {
    return AliCloudProvider.ID;
  }

  @Override
  public List<? extends Item> list() {
    return null;
  }

  @Override
  public Item get(String name) {
    return null;
  }

  private static boolean applicationMatcher(String key, String applicationName) {
    String regex1 = AliCloudProvider.ID + ":.*:" + applicationName + "-.*";
    String regex2 = AliCloudProvider.ID + ":.*:" + applicationName;
    String regex3 = AliCloudProvider.ID + ":.*:" + applicationName + ":.*";
    return Pattern.matches(regex1, key)
        || Pattern.matches(regex2, key)
        || Pattern.matches(regex3, key);
  }
}
