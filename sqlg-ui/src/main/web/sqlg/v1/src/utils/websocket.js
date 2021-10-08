import $ from "jquery";
import CmMithrilGlobal from "./CmMithrilGlobal";
import ReconnectingWebSocket from 'reconnecting-websocket';
import m from "mithril";

const Websocket = {

    //this corresponds to NotificationWebsocketSessionManager.MESSAGE_TYPE in cm-restlet-common
    MESSAGE_TYPE: Object.freeze(
        {
            PING: "ping",
            PONG: "pong",
            NOTIFICATION: "NOTIFICATION",
            UI: "UI"
        }),

    DOWNSTREAM_WEBSOCKET: Object.freeze({
        CMJETTY: "CMJETTY",
        ETL_SERVER: "ETL_SERVER",
        NETVU: "NETVU",
        TRANSMISSION: "TRANSMISSION"
    }),

    DEFAULT_MESSAGE: {
        type: "PONG",
        server: "ETL_SERVER",
        message: "Failed to connect to CMJETTY",
        totalMemory: 0,
        usedMemory: "-",
        usedMemoryPerc: "0%",
        dateTime: "-",
        status: 0
    },

    jettyWebsocket: undefined,
    jettyPingTimer: undefined,
    isOpen: false,
    waitingForCmJettyPong: true,

    sessionExpireToast: function () {
        CmMithrilGlobal.toasts.push({
            type: "warning",
            message: "Your session expired.",
            header: "cm",
            autohide: false
        });
    },

    testJetty: function () {
        function jettyPing() {
            if (document.cookie.indexOf("SqlgToken") !== -1) {
                Websocket.jettyWebsocket.send(JSON.stringify({
                    server: Websocket.DOWNSTREAM_WEBSOCKET.CMJETTY,
                    type: Websocket.MESSAGE_TYPE.PING,
                    message: "ping cmjetty from the browser"
                }));
            } else {
                console.log("redirect");
            }
            // if (document.cookie.indexOf(CmMithrilGlobal.CMTOKEN_COOKIE) === -1) {
            //     CmMithrilGlobal.removeAuthenticationCookies();
            //     Websocket.sessionExpireToast();
            //     m.route.set("/authentication/loginForm", {redirect: m.route.get()});
            // } else {
            // }
        }

        jettyPing();
        if (Websocket.jettyPingTimer === undefined) {
            Websocket.jettyPingTimer = window.setInterval(jettyPing, 5000);
        }
    },

    disconnect: function () {
        if (Websocket.jettyPingTimer !== undefined && Websocket.jettyPingTimer !== null) {
            window.clearInterval(Websocket.jettyPingTimer);
            Websocket.jettyPingTimer = undefined;
        }
        if (Websocket.jettyWebsocket !== undefined && Websocket.jettyWebsocket !== null) {
            Websocket.onclose();
            Websocket.jettyWebsocket.close();
            Websocket.jettyWebsocket = undefined;
        }
    },

    connect: function () {
        //const options = { debug: true };
        let protocol = document.location.protocol === 'http:' ? "ws" : "wss";
        Websocket.jettyWebsocket = new ReconnectingWebSocket(`${protocol}://${document.domain}:${document.location.port}/sqlg/data/v1/websocket`);
        Websocket.jettyWebsocket.addEventListener('open', () => {
            Websocket.isOpen = true;
            Websocket.testJetty();
        });
        Websocket.jettyWebsocket.addEventListener('message', (event) => {
            Websocket.receive(event.data);
        });
        Websocket.jettyWebsocket.addEventListener('close', (ignore) => {
            if (Websocket.isOpen) {
                Websocket.onclose();
            }
        });
        Websocket.jettyWebsocket.addEventListener('error', (event) => {
            Websocket.onerror(event.data);
        });
    },

    send: function (text) {
        Websocket.jettyWebsocket.send(text);
    },

    receive: function (text) {
        let message = JSON.parse(text);
        if (message.type === Websocket.MESSAGE_TYPE.NOTIFICATION) {
            $.Topic('/sqlg-ui/notification').publish(message);
        } else if (message.type === Websocket.MESSAGE_TYPE.UI) {
            if (message.subType === undefined) {
                $.Topic('ui').publish(message);
            } else {
                $.Topic('/sqlg-ui/' + message.subType).publish(message);
            }
        } else if (message.type === Websocket.MESSAGE_TYPE.PING) {
            if (message.server === Websocket.DOWNSTREAM_WEBSOCKET.CMJETTY) {
                //noop
            }
        } else if (message.type === Websocket.MESSAGE_TYPE.PONG) {
            if (message.server === Websocket.DOWNSTREAM_WEBSOCKET.CMJETTY) {
                if (message.status === 0 || message.status === 1) {
                    CmMithrilGlobal.isCmJettyUp = message.status === 1;
                    $.Topic('cmJetty').publish(message);
                } else {
                    CmMithrilGlobal.isCmJettyUp = false;
                    console.error("Invalid server status, only 0 and 1 expected, got " + message.status);
                }
            }
        }
    },

    onclose: function () {
        console.log('Received websocket close from server!');
        Websocket.isOpen = false;
        $.Topic('cmJetty').publish(Websocket.DEFAULT_MESSAGE);
    },

    onerror: function (e) {
        console.log('Received websocket onerror from server!' + e);
    }

};
export default Websocket;
