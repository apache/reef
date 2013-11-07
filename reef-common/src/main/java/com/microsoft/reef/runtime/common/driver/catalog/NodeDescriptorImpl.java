/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver.catalog;

import com.microsoft.reef.driver.capabilities.CPU;
import com.microsoft.reef.driver.capabilities.Capability;
import com.microsoft.reef.driver.capabilities.RAM;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.RackDescriptor;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class NodeDescriptorImpl implements NodeDescriptor {

	private final RackDescriptorImpl rack;
	
	private final String id;
	
	private final InetSocketAddress address;
	
	private final RAM ram;
	
	private final List<Capability> capabilities;
	
	public NodeDescriptorImpl(String id, InetSocketAddress address, RackDescriptorImpl rack, RAM ram) {
		this.id = id;
		this.address = address;
		this.rack = rack;
		this.ram = ram;
		this.capabilities = new ArrayList<>();
		
		this.rack.addNodeDescriptor(this);
	}
	
	@Override
	public String toString() {
		return "Node [" + this.address + "]: RACK " + this.rack.getName() + ", RAM " + ram;
	}
	
	@Override
	public final String getId() {
		return this.id;
	}
	
	@Override
	public List<Capability> getCapabilities() {
		return this.capabilities;
	}

	@Override
	public InetSocketAddress getInetSocketAddress() {
		return this.address;
	}

	@Override
	public CPU getCPUCapability() {
		return new CPU(1); // TODO: fix me, note YARN does not tell me this info
	}

	@Override
	public RAM getRAMCapability() {
		return this.ram;
	}

	@Override
	public RackDescriptor getRackDescriptor() {
		return this.rack;
	}

	@Override
	public String getName() {
		return this.address.getHostName();
	}

}
