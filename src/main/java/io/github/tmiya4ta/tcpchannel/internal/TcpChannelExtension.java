package io.github.tmiya4ta.tcpchannel.internal;

import io.github.tmiya4ta.tcpchannel.internal.config.TcpChannelConfiguration;
import io.github.tmiya4ta.tcpchannel.internal.operations.TcpChannelErrors;
import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.error.ErrorTypes;
import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;
import org.mule.sdk.api.annotation.JavaVersionSupport;
import org.mule.sdk.api.meta.JavaVersion;

@Extension(name = "TCP Channel")
@Xml(prefix = "tcpc")
@Configurations(TcpChannelConfiguration.class)
@ErrorTypes(TcpChannelErrors.class)
@JavaVersionSupport({JavaVersion.JAVA_17})
public class TcpChannelExtension {
}
