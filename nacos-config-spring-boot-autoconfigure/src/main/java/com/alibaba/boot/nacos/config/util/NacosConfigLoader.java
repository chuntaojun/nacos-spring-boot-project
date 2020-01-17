/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.boot.nacos.config.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.alibaba.boot.nacos.config.common.StringConstant;
import com.alibaba.boot.nacos.config.properties.NacosConfigProperties;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.spring.core.env.NacosPropertySource;
import com.alibaba.nacos.spring.core.env.NacosPropertySourcePostProcessor;
import com.alibaba.nacos.spring.util.NacosUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;

import static com.alibaba.nacos.spring.util.NacosUtils.buildDefaultPropertySourceName;

/**
 * @author <a href="mailto:liaochunyhm@live.com">liaochuntao</a>
 * @since 0.2.3
 */
public class NacosConfigLoader {

	private final Logger logger = LoggerFactory.getLogger(NacosConfigLoader.class);

	private final NacosConfigProperties nacosConfigProperties;
	private final ConfigurableEnvironment environment;
	private Function<Properties, ConfigService> builder;
	private List<DeferNacosPropertySource> nacosPropertySources = new LinkedList<>();
	private Properties globalProperties;

	public NacosConfigLoader(NacosConfigProperties nacosConfigProperties,
			ConfigurableEnvironment environment,
			Function<Properties, ConfigService> builder) {
		this.nacosConfigProperties = nacosConfigProperties;
		this.environment = environment;
		this.builder = builder;
	}

	public void loadConfig() {
		this.globalProperties = buildGlobalNacosProperties();
		MutablePropertySources mutablePropertySources = environment.getPropertySources();
		List<NacosPropertySource> sources = reqGlobalNacosConfig(globalProperties);
		for (NacosConfigProperties.Config config : nacosConfigProperties.getExtConfig()) {
			List<NacosPropertySource> elements = reqSubNacosConfig(config,
					globalProperties);
			sources.addAll(elements);
		}
		for (NacosPropertySource propertySource : sources) {
			mutablePropertySources.addLast(propertySource);
		}
	}

	public Properties getGlobalProperties() {
		return globalProperties;
	}

	private Properties buildGlobalNacosProperties() {
		return NacosPropertiesBuilder.buildNacosProperties(environment,
				nacosConfigProperties.getServerAddr(),
				nacosConfigProperties.getNamespace(), nacosConfigProperties.getEndpoint(),
				nacosConfigProperties.getSecretKey(),
				nacosConfigProperties.getAccessKey(),
				nacosConfigProperties.getRamRoleName(),
				nacosConfigProperties.getConfigLongPollTimeout(),
				nacosConfigProperties.getConfigRetryTime(),
				nacosConfigProperties.getMaxRetry(),
				nacosConfigProperties.isEnableRemoteSyncConfig());
	}

	private Properties buildSubNacosProperties(Properties globalProperties,
			NacosConfigProperties.Config config) {
		Properties sub = NacosPropertiesBuilder.buildNacosProperties(environment,
				config.getServerAddr(), config.getNamespace(), config.getEndpoint(),
				config.getSecretKey(), config.getAccessKey(), config.getRamRoleName(),
				config.getConfigLongPollTimeout(), config.getConfigRetryTime(),
				config.getMaxRetry(), config.isEnableRemoteSyncConfig());
		NacosPropertiesBuilder.merge(sub, globalProperties);
		return sub;
	}

	private List<NacosPropertySource> reqGlobalNacosConfig(Properties globalProperties) {
		final String dataIds = Optional.ofNullable(nacosConfigProperties.getDataId()).orElse("")
				+ StringConstant.COMMA
				+ Optional.ofNullable(nacosConfigProperties.getDataIds()).orElse("");
		return reqConfig(globalProperties, dataIds,
				nacosConfigProperties.getGroup(), nacosConfigProperties.getType(),
				nacosConfigProperties.isAutoRefresh());
	}

	private List<NacosPropertySource> reqSubNacosConfig(
			NacosConfigProperties.Config config, Properties globalProperties) {
		Properties subConfigProperties = buildSubNacosProperties(globalProperties,
				config);
		final String dataIds = Optional.ofNullable(config.getDataId()).orElse("")
				+ StringConstant.COMMA
				+ Optional.ofNullable(config.getDataIds()).orElse("");
		return reqConfig(subConfigProperties, dataIds, config.getGroup(),
				config.getType(), config.isAutoRefresh());
	}

	private List<NacosPropertySource> reqConfig(Properties configProperties,
			String dataIds, String groupId, ConfigType type, boolean isAutoRefresh) {
		List<String> findDataIds = new ArrayList<>();
		if (StringUtils.isAllBlank(dataIds)
				|| dataIds.trim().equals(StringConstant.COMMA)) {
			return Collections.emptyList();
		}
		if (dataIds.contains(StringConstant.COMMA)) {
			final String ids = environment.resolvePlaceholders(dataIds);
			findDataIds.addAll(Arrays.asList(ids.split(StringConstant.COMMA)));
		}
		else {
			findDataIds.add(dataIds);
		}
		findDataIds = findDataIds.stream().filter(StringUtils::isNoneBlank).collect(
				Collectors.toList());
		final String groupName = environment.resolvePlaceholders(groupId);
		return new ArrayList<>(Arrays.asList(reqNacosConfig(configProperties,
				findDataIds.toArray(new String[0]), groupName, type, isAutoRefresh)));
	}

	private NacosPropertySource[] reqNacosConfig(Properties configProperties,
			String[] dataIds, String groupId, ConfigType type, boolean isAutoRefresh) {
		final NacosPropertySource[] propertySources = new NacosPropertySource[dataIds.length];
		for (int i = 0; i < dataIds.length; i++) {
			if (StringUtils.isBlank(dataIds[i])) {
				continue;
			}

			// Remove excess Spaces

			final String dataId = environment.resolvePlaceholders(dataIds[i].trim());
			final String config = NacosUtils.getContent(builder.apply(configProperties),
					dataId, groupId);
			final NacosPropertySource nacosPropertySource = new NacosPropertySource(
					dataId, groupId,
					buildDefaultPropertySourceName(dataId, groupId, configProperties),
					config, type.getType());
			nacosPropertySource.setDataId(dataId);
			nacosPropertySource.setType(type.getType());
			nacosPropertySource.setGroupId(groupId);
			nacosPropertySource.setAutoRefreshed(isAutoRefresh);
			logger.info("load config from nacos, data-id is : {}, group is : {}",
					nacosPropertySource.getDataId(), nacosPropertySource.getGroupId());
			propertySources[i] = nacosPropertySource;
			DeferNacosPropertySource defer = new DeferNacosPropertySource(
					nacosPropertySource, configProperties, environment);

			// Only those with a refresh listener are added to the delay queue

			if (isAutoRefresh) {
				nacosPropertySources.add(defer);
			}
		}
		return propertySources;
	}

	public void addListenerIfAutoRefreshed() {
		addListenerIfAutoRefreshed(nacosPropertySources);
	}

	public void addListenerIfAutoRefreshed(
			final List<DeferNacosPropertySource> deferNacosPropertySources) {
		for (DeferNacosPropertySource deferNacosPropertySource : deferNacosPropertySources) {
			NacosPropertySourcePostProcessor.addListenerIfAutoRefreshed(
					deferNacosPropertySource.getNacosPropertySource(),
					deferNacosPropertySource.getProperties(),
					deferNacosPropertySource.getEnvironment());
		}
	}

	public List<DeferNacosPropertySource> getNacosPropertySources() {
		return nacosPropertySources;
	}

	// Delay Nacos configuration data source object, used for log level of loading time,
	// the cache configuration, wait for after the completion of the Spring Context
	// created in the release

	public static class DeferNacosPropertySource {

		private final NacosPropertySource nacosPropertySource;
		private final ConfigurableEnvironment environment;
		private final Properties properties;

		DeferNacosPropertySource(NacosPropertySource nacosPropertySource,
				Properties properties, ConfigurableEnvironment environment) {
			this.nacosPropertySource = nacosPropertySource;
			this.properties = properties;
			this.environment = environment;
		}

		NacosPropertySource getNacosPropertySource() {
			return nacosPropertySource;
		}

		ConfigurableEnvironment getEnvironment() {
			return environment;
		}

		public Properties getProperties() {
			return properties;
		}
	}
}
