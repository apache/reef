/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.group.config;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.IdentifierFactory;

/**
 * 
 */
public class ActivityTreeImpl implements ActivityTree {
  
  private static class IdentifierStatus{
    public final ComparableIdentifier id;
    public Status status;
    
    @Override
    public String toString() {
      return "(" + id + ", " + status + ")"; 
    }
    
    @Override
    public boolean equals(Object obj) {
      if(obj==null || !(obj instanceof IdentifierStatus))
        return false;
      if(obj==this)
        return true;
      IdentifierStatus that = (IdentifierStatus) obj;
      if(this.status!=Status.ANY && that.status!=Status.ANY){
        return this.id.equals(that.id) && this.status.equals(that.status); 
      }
      else{
        return this.id.equals(that.id);
      }
    }
    
    public IdentifierStatus(ComparableIdentifier id) {
      super();
      this.id = id;
      this.status = Status.UNSCHEDULED;
    }

    public IdentifierStatus(ComparableIdentifier id, Status status) {
      super();
      this.id = id;
      this.status = status;
    }
    
    public static IdentifierStatus any(ComparableIdentifier id){
      return new IdentifierStatus(id, Status.ANY);
    }
  }
  
  private final List<IdentifierStatus> activities = new ArrayList<>();
  private final TreeSet<Integer> holes = new TreeSet<>();

  @Override
  public synchronized void add(ComparableIdentifier id) {
    System.out.println("Before Activities: " + activities);
//    System.out.println("Before Holes: " + holes);
    if(holes.isEmpty()){
//      System.out.println("No holes. Adding at the end");
      activities.add(new IdentifierStatus(id));
    }
    else{
//      System.out.println("Found holes: " + holes);
      int firstHole = holes.first();
//      System.out.println("First hole at " + firstHole + ". Removing it");
      holes.remove(firstHole);
//      System.out.println("Adding " + id + " at " + firstHole);
      activities.add(firstHole, new IdentifierStatus(id));
    }
    System.out.println("After Activities: " + activities);
//    System.out.println("After Holes: " + holes);
  }
  
  @Override
  public String toString() {
    return activities.toString();
  }

  @Override
  public ComparableIdentifier parent(ComparableIdentifier id) {
    int idx = activities.indexOf(IdentifierStatus.any(id));
    if(idx==-1 || idx==0)
      return null;
    int parIdx = (idx-1) / 2;
    try{
      return activities.get(parIdx).id;
    }catch(IndexOutOfBoundsException e){
      return null;
    }
  }

  @Override
  public ComparableIdentifier left(ComparableIdentifier id) {
    int idx = activities.indexOf(IdentifierStatus.any(id));
    if(idx==-1)
      return null;
    int leftIdx = idx * 2 + 1;
    try {
      return activities.get(leftIdx).id;
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public ComparableIdentifier right(ComparableIdentifier id) {
    int idx = activities.indexOf(IdentifierStatus.any(id));
    if(idx==-1)
      return null;
    int rightIdx = idx * 2 + 2;
    try{
      return activities.get(rightIdx).id;
    }catch(IndexOutOfBoundsException e){
      return null;
    }
  }

  @Override
  public List<ComparableIdentifier> neighbors(ComparableIdentifier id) {
    List<ComparableIdentifier> retVal = new ArrayList<>();
    ComparableIdentifier parent = parent(id);
    if(parent!=null)
      retVal.add(parent);
    retVal.addAll(children(id));
    return retVal;
  }

  @Override
  public List<ComparableIdentifier> children(ComparableIdentifier id) {
    List<ComparableIdentifier> retVal = new ArrayList<>();
    ComparableIdentifier left = left(id);
    if(left!=null){
      retVal.add(left);
      ComparableIdentifier right = right(id);
      if(right!=null)
        retVal.add(right);
    }
    return retVal;
  }

  @Override
  public int childrenSupported(ComparableIdentifier actId) {
    return 2;
  }

  @Override
  public synchronized void remove(ComparableIdentifier failedActId) {
    int hole = activities.indexOf(IdentifierStatus.any(failedActId));
    if(hole==-1)
      return;
    holes.add(hole);
    activities.remove(hole);
  }

  @Override
  public List<ComparableIdentifier> scheduledChildren(ComparableIdentifier actId) {
    List<ComparableIdentifier> children = children(actId);
    List<ComparableIdentifier> retVal = new ArrayList<>();
    for (ComparableIdentifier child : children) {
      Status s = getStatus(child); 
      if(Status.SCHEDULED==s)
        retVal.add(child);
    }
    return retVal;
  }

  @Override
  public List<ComparableIdentifier> scheduledNeighbors(
      ComparableIdentifier actId) {
    List<ComparableIdentifier> neighbors = neighbors(actId);
    List<ComparableIdentifier> retVal = new ArrayList<>();
    for (ComparableIdentifier neighbor : neighbors) {
      Status s = getStatus(neighbor); 
      if(Status.SCHEDULED==s)
        retVal.add(neighbor);      
    }
    return retVal;
  }

  @Override
  public void setStatus(ComparableIdentifier actId, Status status) {
    int idx = activities.indexOf(IdentifierStatus.any(actId));
    if(idx==-1)
      return;
    activities.get(idx).status = status;
  }

  @Override
  public Status getStatus(ComparableIdentifier actId) {
    int idx = activities.indexOf(IdentifierStatus.any(actId));
    if(idx==-1)
      return null;
    return activities.get(idx).status;
  }
  
  
  public static void main(String[] args) {
    IdentifierFactory idFac = new StringIdentifierFactory();
    ActivityTree tree = new ActivityTreeImpl();
    ComparableIdentifier[] ids = new ComparableIdentifier[7];
    for(int i=0;i<ids.length;i++){
      ids[i] = (ComparableIdentifier) idFac.getNewInstance(Integer.toString(i));
    }
    tree.add(ids[0]);
    tree.add(ids[2]);
    tree.add(ids[1]);
    tree.add(ids[6]);
    tree.add(ids[4]);
    tree.add(ids[5]);
    //tree.add(ids[3]);
    
    tree.setStatus(ids[2], Status.SCHEDULED);
    
    System.out.println(tree);
    System.out.println("Children");
    System.out.println(tree.children(ids[0]));
    System.out.println("Sched Children");
    System.out.println(tree.scheduledChildren(ids[0]));
    System.out.println();
    
    Queue<ComparableIdentifier> idss = new LinkedList<>();
    idss.add((ComparableIdentifier) idFac.getNewInstance("0"));
    while(!idss.isEmpty()){
      ComparableIdentifier id = idss.poll();
      System.out.println(id);
      ComparableIdentifier left = tree.left(id);
      if(left!=null){
        idss.add(left);
        ComparableIdentifier right = tree.right(id);
        if(right!=null)
          idss.add(right);
      }
    }
    tree.setStatus(ids[4], Status.SCHEDULED);
    System.out.println(tree);
    System.out.println("Neighbors");
    System.out.println(tree.neighbors(ids[2]));
    System.out.println("Sched Neighbors");
    System.out.println(tree.scheduledNeighbors(ids[2]));
  }
}
