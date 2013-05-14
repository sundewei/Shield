package com.sap.shield.cache;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 11/13/12
 * Time: 8:47 PM
 * To change this template use File | Settings | File Templates.
 */
public interface WorkStatus {
    public boolean isActive();

    public void setActive(boolean active);

    public boolean isSlaveMachine();

    public void setSlaveMachine(boolean slaveMachine);
}
