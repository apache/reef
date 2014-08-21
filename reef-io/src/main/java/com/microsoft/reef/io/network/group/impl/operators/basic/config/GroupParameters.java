/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.io.network.group.impl.operators.basic.config;

import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

/**
 * Parameters for the basic implementation of the group operators
 * 
 * The parameters are grouped based on the operator and whether they are for
 * Sender or Receiver.
 */
public class GroupParameters {
  public static final String defaultValue = "NULL";
  
	@NamedParameter(doc = "IdentifierFactory to use to create Identifiers out of strings", default_class=StringIdentifierFactory.class)
	public static class IDFactory implements Name<IdentifierFactory> {
		//intentionally blank
	}
	
	public static final Tang tang = Tang.Factory.getTang();
	
	public static class Scatter {
		@NamedParameter(doc = "codec for the data to be used")
		public static class DataCodec implements Name<Codec<?>> {
			//intentionally blank
		}

		public static class SenderParams {
			@NamedParameter(doc = "Task ID of the sender")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the sender", default_value=defaultValue)
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the sender sends to")
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
		}
		public static class ReceiverParams{
			@NamedParameter(doc = "Task ID of the receiver")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the receiver")
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the receiver receives from", default_value=defaultValue)
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
		}
	}
	
	public static class Gather {
		@NamedParameter(doc = "codec for the data to be used")
		public static class DataCodec implements Name<Codec<?>> {
			//intentionally blank
		}

		public static class SenderParams {
			@NamedParameter(doc = "Task ID of the sender")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the sender")
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the sender sends to", default_value=defaultValue)
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
		}
		public static class ReceiverParams{
			@NamedParameter(doc = "Task ID of the receiver")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the receiver", default_value=defaultValue)
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the receiver receives from")
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
		}
	}
	
	public static class BroadCast {
		@NamedParameter(doc = "codec for the data to be used")
		public static class DataCodec implements Name<Codec<?>> {
			//intentionally blank
		}
		
		public static class SenderParams {
			@NamedParameter(doc = "Task ID of the sender")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the sender", default_value=defaultValue)
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the sender sends to")
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
			
			/*public static <T> Configuration build(SenderReceiverConfig<T> sndRcvConf) throws BindException{
				JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
				jcb.bindNamedParameter(SelfId.class, (sndRcvConf.getSelf()==null) ? "" : sndRcvConf.getSelf().toString());
				jcb.bindNamedParameter(ParentId.class, (sndRcvConf.getParent()==null) ? "" : sndRcvConf.getParent().toString());
				jcb.bindNamedParameter(ChildIds.class, Utils.listToString(sndRcvConf.getChildren()));
				jcb.bindNamedParameter(DataCodec.class, sndRcvConf.dataCodec);
				jcb.bindImplementation(com.microsoft.reef.io.network.group.operators.Broadcast.Sender.class, BroadcastOp.Sender.class);
				return jcb.build();
			}*/
		}
		public static class ReceiverParams{
			@NamedParameter(doc = "Task ID of the receiver")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the receiver")
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the receiver receives from", default_value=defaultValue)
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
			
			/*public static <T> Configuration build(SenderReceiverConfig<T> sndRcvConf) throws BindException{
				JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
				jcb.bindNamedParameter(SelfId.class, (sndRcvConf.getSelf()==null) ? "" : sndRcvConf.getSelf().toString());
				jcb.bindNamedParameter(ParentId.class, (sndRcvConf.getParent()==null) ? "" : sndRcvConf.getParent().toString());
				jcb.bindNamedParameter(ChildIds.class, Utils.listToString(sndRcvConf.getChildren()));
				jcb.bindNamedParameter(DataCodec.class, sndRcvConf.dataCodec);
				jcb.bindImplementation(com.microsoft.reef.io.network.group.operators.Broadcast.Receiver.class, BroadcastOp.Receiver.class);
				return jcb.build();
			}*/
		}

	}
	
	public static class Reduce {
		@NamedParameter(doc = "codec for the data to be used")
		public static class DataCodec implements Name<Codec<?>> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Reduce function to be used")
		public static class ReduceFunction implements Name<com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction<?>> {
			//intentionally blank
		}

		public static class SenderParams {
			@NamedParameter(doc = "Task ID of the sender")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the sender")
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the sender sends to", default_value=defaultValue)
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
		}
		public static class ReceiverParams{
			@NamedParameter(doc = "Task ID of the receiver")
			public static class SelfId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "Task ID of the parent of the receiver", default_value=defaultValue)
			public static class ParentId implements Name<String> {
				//intentionally blank
			}
			
			@NamedParameter(doc = "List of child Identifiers that the receiver receives from")
			public static class ChildIds implements Name<String> {
				//intentionally blank
			}
		}
	}
	
	public static class AllGather{
		@NamedParameter(doc = "codec for the data to be used")
		public static class DataCodec implements Name<Codec<?>> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Task ID of the operator")
		public static class SelfId implements Name<String> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Task ID of the parent of the operator", default_value=defaultValue)
		public static class ParentId implements Name<String> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "List of child Identifiers that the operator sends to", default_value=defaultValue)
		public static class ChildIds implements Name<String> {
			//intentionally blank
		}
	}
	
	public static class AllReduce{
		@NamedParameter(doc = "codec for the data to be used")
		public static class DataCodec implements Name<Codec<?>> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Reduce function to be used")
		public static class ReduceFunction implements Name<com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction<?>> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Task ID of the operator")
		public static class SelfId implements Name<String> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Task ID of the parent of the operator", default_value=defaultValue)
		public static class ParentId implements Name<String> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "List of child Identifiers that the operator sends to", default_value=defaultValue)
		public static class ChildIds implements Name<String> {
			//intentionally blank
		}
	}
	
	public static class ReduceScatter{
		@NamedParameter(doc = "codec for the data to be used")
		public static class DataCodec implements Name<Codec<?>> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Reduce function to be used")
		public static class ReduceFunction implements Name<com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction<?>> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Task ID of the operator")
		public static class SelfId implements Name<String> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "Task ID of the parent of the operator", default_value=defaultValue)
		public static class ParentId implements Name<String> {
			//intentionally blank
		}
		
		@NamedParameter(doc = "List of child Identifiers that the operator sends to", default_value=defaultValue)
		public static class ChildIds implements Name<String> {
			//intentionally blank
		}
	}
}
