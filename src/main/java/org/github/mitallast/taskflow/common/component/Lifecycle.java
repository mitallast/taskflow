package org.github.mitallast.taskflow.common.component;

public class Lifecycle {

    private volatile State state = State.INITIALIZED;

    public State state() {
        return this.state;
    }

    /**
     * Returns <tt>true</tt> if the state is initialized.
     */
    public boolean initialized() {
        return state == State.INITIALIZED;
    }

    /**
     * Returns <tt>true</tt> if the state is started.
     */
    public boolean started() {
        return state == State.STARTED;
    }

    /**
     * Returns <tt>true</tt> if the state is stopped.
     */
    public boolean stopped() {
        return state == State.STOPPED;
    }

    /**
     * Returns <tt>true</tt> if the state is closed.
     */
    public boolean closed() {
        return state == State.CLOSED;
    }

    public boolean stoppedOrClosed() {
        Lifecycle.State state = this.state;
        return state == State.STOPPED || state == State.CLOSED;
    }

    public boolean canMoveToStarted() throws IllegalStateException {
        State localState = this.state;
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return true;
        }
        if (localState == State.STARTED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to started state when closed");
        }
        throw new IllegalStateException("Can't move to started with unknown state");
    }

    public boolean moveToStarted() throws IllegalStateException {
        State localState = this.state;
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            state = State.STARTED;
            return true;
        }
        if (localState == State.STARTED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to started state when closed");
        }
        throw new IllegalStateException("Can't move to started with unknown state");
    }

    public boolean canMoveToStopped() throws IllegalStateException {
        State localState = state;
        if (localState == State.STARTED) {
            return true;
        }
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to started state when closed");
        }
        throw new IllegalStateException("Can't move to started with unknown state");
    }

    public boolean moveToStopped() throws IllegalStateException {
        State localState = state;
        if (localState == State.STARTED) {
            state = State.STOPPED;
            return true;
        }
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to started state when closed");
        }
        throw new IllegalStateException("Can't move to started with unknown state");
    }

    public boolean canMoveToClosed() throws IllegalStateException {
        State localState = state;
        if (localState == State.CLOSED) {
            return false;
        }
        if (localState == State.STARTED) {
            throw new IllegalStateException("Can't move to closed before moving to stopped mode");
        }
        return true;
    }

    public boolean moveToClosed() throws IllegalStateException {
        State localState = state;
        if (localState == State.CLOSED) {
            return false;
        }
        if (localState == State.STARTED) {
            throw new IllegalStateException("Can't move to closed before moving to stopped mode");
        }
        state = State.CLOSED;
        return true;
    }

    @Override
    public String toString() {
        return state.toString();
    }

    public enum State {
        INITIALIZED,
        STOPPED,
        STARTED,
        CLOSED
    }
}