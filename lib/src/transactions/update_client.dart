import 'dart:async';

import 'package:sip_ua/src/constants.dart';

import '../event_manager/event_manager.dart';
import '../event_manager/internal_events.dart';
import '../logger.dart';
import '../sip_message.dart';
import '../socket_transport.dart';
import '../timers.dart';
import '../ua.dart';
import '../utils.dart';
import 'transaction_base.dart';

class UpdateClientTransaction extends TransactionBase {
  UpdateClientTransaction(UA ua, SocketTransport transport,
      OutgoingRequest request, EventManager eventHandlers) {
    id = 'z9hG4bK${Math.random().floor()}';
    this.ua = ua;
    this.transport = transport;
    this.request = request;
    _eventHandlers = eventHandlers;

    String via = 'SIP/2.0/${transport.via_transport}';

    via += ' ${ua.configuration.via_host};branch=$id';

    request.setHeader('via', via);

    ua.newTransaction(this);
  }

  late EventManager _eventHandlers;
  Timer? F, K, R;

  bool resubmitByTransportIssue = false;

  void stateChanged(TransactionState state) {
    this.state = state;
    emit(EventStateChanged());
  }

  @override
  void send() {
    stateChanged(TransactionState.TRYING);
    F = setTimeout(() {
      timer_F();
    }, Timers.TIMER_F);

    if (!transport!.send(request)) {
      logger.d('transport issue: unable to send transaction (${request?.method}, $id), resubmitByTransportIssue: $resubmitByTransportIssue');

      if(resubmitByTransportIssue){
        logger.d('transaction (${request?.method}, $id) fail, report transport error');
        onTransportError();
      }
      else{
        logger.d('reschedule transaction (${request?.method}, $id) in 2 sec');
        R = setTimeout(() {
          timer_R();
        }, Timers.TIMER_R);
      }
    }
    else{
      logger.d('transaction (${request?.method}, $id) sent');
    }
  }

  @override
  void onTransportError() {
    logger.d('transport error occurred (${request?.method}, $id)');
    clearTimeout(F);
    clearTimeout(K);
    clearTimeout(R);
    if(resubmitByTransportIssue) {
      logger.d('deleting transaction (${request?.method}, $id)');
      stateChanged(TransactionState.TERMINATED);
      ua.destroyTransaction(this);

      //report issue for non UPDATE transactions
      if(request?.method != SipMethod.UPDATE) {
        logger.d('report transport issue');
        _eventHandlers.emit(EventOnTransportError());
      }
    }
    else{
      logger.d('reschedule transaction (${request?.method}, $id) in 2 sec');
      R = setTimeout(() {
        timer_R();
      }, Timers.TIMER_R);
    }
  }

  void timer_F() {
    logger.d('Timer F expired for transaction (${request?.method}, $id), resubmitByTransportIssue: $resubmitByTransportIssue');
    if(resubmitByTransportIssue) {
      logger.d('transaction (${request?.method}, $id) fail, request timeout');
      stateChanged(TransactionState.TERMINATED);
      ua.destroyTransaction(this);

      //report issue for non UPDATE transactions
      if(request?.method != SipMethod.UPDATE) {
        logger.d('report transport issue');
        _eventHandlers.emit(EventOnRequestTimeout());
      }
    }else{
      logger.d('reschedule transaction (${request?.method}, $id)');
      resubmitByTransportIssue = true;
      send();
    }
  }

  void timer_K() {
    stateChanged(TransactionState.TERMINATED);
    ua.destroyTransaction(this);
  }

  void timer_R() {
    resubmitByTransportIssue = true;
    send();
  }

  @override
  void receiveResponse(int status_code, IncomingMessage response,
      [void Function()? onSuccess, void Function()? onFailure]) {
    if (status_code < 200) {
      switch (state) {
        case TransactionState.TRYING:
        case TransactionState.PROCEEDING:
          stateChanged(TransactionState.PROCEEDING);
          _eventHandlers.emit(
              EventOnReceiveResponse(response: response as IncomingResponse?));
          break;
        default:
          break;
      }
    } else {
      switch (state) {
        case TransactionState.TRYING:
        case TransactionState.PROCEEDING:
          stateChanged(TransactionState.COMPLETED);
          clearTimeout(F);

          if (status_code == 408) {
            _eventHandlers.emit(EventOnRequestTimeout());
          } else {
            _eventHandlers.emit(EventOnReceiveResponse(
                response: response as IncomingResponse?));
          }

          K = setTimeout(() {
            timer_K();
          }, Timers.TIMER_K);
          break;
        case TransactionState.COMPLETED:
          break;
        default:
          break;
      }
    }
  }
}
