package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2016/04/18
 * Time: 7:40 PM
 */
public class OptionalLeaveNodes<E extends SqlgElement> {

    private List<Path> paths = new ArrayList<>();
    private int numberOfSteps;

    public OptionalLeaveNodes(int numberOfSteps) {
        this.numberOfSteps = numberOfSteps;
    }

    public void addPath(Path path) {
        this.paths.add(path);
    }

    public boolean containsPath(Path path) {
        boolean result = false;
        if (!this.paths.isEmpty()) {
            List<Object> pathToAddObjects = path.objects();
            for (Path p : this.paths) {
                List<Object> objects = p.objects();
                if (pathToAddObjects.size() < this.numberOfSteps) {
                    result = true;
                    int count = 0;
                    for (Object pathToAddObject : pathToAddObjects) {
                        if (!pathToAddObject.equals(objects.get(count++))) {
                            result = false;
                            break;
                        }
                    }
                }
                if (result) {
                    break;
                }
            }
        }
        return result;
    }
}
