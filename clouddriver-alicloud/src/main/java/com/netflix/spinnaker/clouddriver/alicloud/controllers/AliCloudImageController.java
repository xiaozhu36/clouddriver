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

package com.netflix.spinnaker.clouddriver.alicloud.controllers;

import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.IMAGES;

import com.netflix.spinnaker.cats.cache.Cache;
import com.netflix.spinnaker.cats.cache.CacheData;
import com.netflix.spinnaker.clouddriver.alicloud.cache.Keys;
import groovy.util.logging.Slf4j;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/alicloud/images")
public class AliCloudImageController {

  private final Cache cacheView;

  @Autowired
  public AliCloudImageController(Cache cacheView) {
    this.cacheView = cacheView;
  }

  @RequestMapping(value = "/find", method = RequestMethod.GET)
  List<Image> list(LookupOptions lookupOptions) {
    List<Image> list = new ArrayList<>();

    String glob = lookupOptions.q;
    glob = glob + "*";
    String imageSearchKey = Keys.getImageKey(glob, "*", "*");
    Collection<String> imageIdentifiers = cacheView.filterIdentifiers(IMAGES.ns, imageSearchKey);
    Collection<CacheData> images = cacheView.getAll(IMAGES.ns, imageIdentifiers, null);
    for (CacheData image : images) {
      Map<String, Object> attributes = image.getAttributes();
      list.add(new Image(String.valueOf(attributes.get("imageName")), attributes));
    }

    return list;
  }

  @Data
  public static class Image {
    public Image(String imageName, Map<String, Object> attributes) {
      this.imageName = imageName;
      this.attributes = attributes;
    }

    String imageName;
    Map<String, Object> attributes;
  }

  @Data
  static class LookupOptions {
    String q;
    String account;
    String region;
  }
}
