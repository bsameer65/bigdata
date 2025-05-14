import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class WebSocketService {
  private socket: WebSocket;

  public connect(): Observable<string> {
    return new Observable<string>(observer => {
      this.socket = new WebSocket('ws://localhost:8000/ws');

      this.socket.onmessage = event => {
        observer.next(event.data);
      };

      this.socket.onerror = error => {
        console.error('WebSocket error:', error);
        observer.error(error);
      };

      this.socket.onclose = () => {
        observer.complete();
      };
    });
  }
}
