import 'dart:async';
import 'dart:io';
import 'package:test/test.dart';
import 'package:sip_ua/src/WebSocketInterface.dart';
import 'package:sip_ua/src/Transport.dart';

var testFunctions = [
  () => test("WebSocket: EchoTest", () async {
        var completer = new Completer();
        HttpServer.bind('127.0.0.1', 4040).then((server) async {
          try {
            await for (var req in server) {
              if (req.uri.path == '/sip') {
                // Upgrade a HttpRequest to a WebSocket connection.
                var socket = await WebSocketTransformer.upgrade(req);
                socket.listen((msg) {
                  socket.add(msg);
                  expect(msg, 'message');
                  socket.close();
                  server.close();
                });
              }
            }
          } catch (error) {
            print("An error occurred. $error");
          }
        });

        WebSocketInterface client =
            new WebSocketInterface('ws://127.0.0.1:4040/sip');

        expect(client.url, 'ws://127.0.0.1:4040/sip');
        expect(client.via_transport, 'WS');
        expect(client.sip_uri, 'sip:127.0.0.1:4040;transport=ws');
        expect(client.isConnected(), false);

        client.onconnect = () {
          print('connected');
          client.send("message");
          expect(client.isConnected(), true);
        };
        client.ondata = (data) async {
          print('ondata => $data');
          expect(data, "message");
          client.disconnect();
          completer.complete();
        };
        client.ondisconnect = (reason) {
          print('ondisconnect => ${reason.toString()}');
          expect(client.isConnected(), false);
        };
        client.connect();
        return completer;
      }),
  () => test("WebSocket: EchoTest", () async {
        var completer = new Completer();
        HttpServer.bind('127.0.0.1', 4041).then((server) async {
          try {
            await for (var req in server) {
              if (req.uri.path == '/sip') {
                // Upgrade a HttpRequest to a WebSocket connection.
                var socket = await WebSocketTransformer.upgrade(req);
                socket.listen((msg) {
                  socket.add(msg);
                  expect(msg, 'message');
                  socket.close();
                  server.close();
                });
              }
            }
          } catch (error) {
            print("An error occurred. $error");
          }
        });
        WebSocketInterface socket =
            new WebSocketInterface('ws://127.0.0.1:4041/sip');
        Transport trasnport = new Transport(socket);

        trasnport.onconnecting = (socket) {
          expect(trasnport.isConnecting(), true);
        };

        trasnport.onconnect = (socket){
          expect(trasnport.isConnected(), true);
          trasnport.send('message');
        };

        trasnport.ondata = (socket) {
          expect(socket['message'], 'message');
          trasnport.disconnect();
        };

        trasnport.ondisconnect = (socket){
          expect(trasnport.isConnected(), false);
          completer.complete();
        };

        trasnport.connect();

        return completer;
      })
];

void main() {
  //testFunctions.forEach((func) => func());
  testFunctions[1]();
}
