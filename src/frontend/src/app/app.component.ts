import { Component, OnInit, NgZone  } from '@angular/core';
import { WebSocketService } from './app.service';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-root',
  imports: [CommonModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss',
})
export class AppComponent implements OnInit {
  title = 'frontend';
  messages: string[] = [];

  constructor(private wsService: WebSocketService, private ngZone: NgZone  ) {
    this.wsService.connect().subscribe((msg) => {
      // Run UI update inside Angular zone
      this.ngZone.run(() => {
        console.log('new msg', msg);
        this.messages.unshift(msg);
        if (this.messages.length > 100) this.messages.pop();
      });
    });
  }

  ngOnInit() {
    // this.wsService.connect().subscribe({
    //   next: (msg: string) => {
    //     console.log('new msg', msg);
    //     this.messages.unshift(msg); // show latest on top
    //   },
    //   error: (err) => console.error('WebSocket error:', err),
    //   complete: () => console.warn('WebSocket closed'),
    // });
  }
}
