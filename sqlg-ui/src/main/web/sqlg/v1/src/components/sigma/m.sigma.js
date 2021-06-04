// Michael Van Der Merwe 16:58pm - 10/12/2018
import m from "mithril";
import CmMithrilGlobal from "../../utils/CmMithrilGlobal"

// window.sigma = require("../../assets/sigma/sigma.require");
// require("../../assets/sigma/plugins/sigma.plugins.animate.min");
// require("../../assets/sigma/plugins/sigma.plugins.dragNodes.min");
// require("../../assets/sigma/plugins/sigma.layout.forceAtlas2.min");
// require("../../assets/sigma/plugins/sigma.renderers.customShapes.min");
// require("../../assets/sigma/plugins/sigma.renderers.edgeLabels.min");
// require("../../assets/sigma/plugins/sigma.renderers.parallelEdges.min");
// require("../../assets/sigma/plugins/sigma.parsers.json.min");

import { UndirectedGraph } from "graphology";
import erdosRenyi from "graphology-generators/random/erdos-renyi";
import randomLayout from "graphology-layout/random";
import faker from "faker";
import WebGLRenderer from "sigma/renderers/webgl/index";
import "./sigma.scss";

function Sigma(ignore) {

    let testData = {
        "nodes": [
            {
                "id": "n0",
                "label": "A node",
                "x": 0,
                "y": 0,
                "size": 3
            },
            {
                "id": "n1",
                "label": "Another node",
                "x": 3,
                "y": 1,
                "size": 2
            },
            {
                "id": "n2",
                "label": "And a last one",
                "x": 1,
                "y": 3,
                "size": 1
            }
        ],
        "edges": [
            {
                "id": "e0",
                "source": "n0",
                "target": "n1"
            },
            {
                "id": "e1",
                "source": "n1",
                "target": "n2"
            },
            {
                "id": "e2",
                "source": "n2",
                "target": "n0"
            }
        ]
    };

    return {
        oncreate:function(){
            const container = document.getElementById("sigma-container");

            const graph = erdosRenyi(UndirectedGraph, { order: 100, probability: 0.2 });
            randomLayout.assign(graph);

            graph.nodes().forEach((node) => {
                graph.mergeNodeAttributes(node, {
                    label: faker.name.findName(),
                    size: Math.max(4, Math.random() * 10),
                    color: '#565656',
                });
            });

            const renderer = new WebGLRenderer(graph, container);

            window.graph = graph;
            window.renderer = renderer;
            window.camera = renderer.camera;

        },
        view: function (v) {
            return m("div#sigma-container");
        }
    }

}

export default Sigma;