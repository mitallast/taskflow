package org.github.mitallast.taskflow.docker;

import com.cronutils.utils.Preconditions;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.core.command.ListContainersCmdImpl;
import com.github.dockerjava.core.util.FiltersBuilder;

import java.lang.reflect.Field;

public class Hack {

    public static FiltersBuilder filters(ListContainersCmd cmd) {
        Preconditions.checkNotNull(cmd);
        ListContainersCmdImpl impl = (ListContainersCmdImpl) cmd;
        try {
            Field field = impl.getClass().getDeclaredField("filters");
            field.setAccessible(true);
            return (FiltersBuilder) field.get(cmd);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
