package com.diecocan.portfolio.fraud.sse;

import com.diecocan.portfolio.fraud.avro.AlertReason;
import com.diecocan.portfolio.fraud.entity.AlertEntity;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class AlertBroadcastServiceTest {

    private final AlertBroadcastService service = new AlertBroadcastService();

    private AlertEntity sampleAlert() {
        return new AlertEntity("a1", "t1", "acct1", AlertReason.VELOCITY, 0.9, Instant.now());
    }

    @SuppressWarnings("unchecked")
    private List<SseEmitter> emitters() {
        return (List<SseEmitter>) ReflectionTestUtils.getField(service, "emitters");
    }

    @Test
    void subscribe_registersANewEmitter() {
        service.subscribe();

        assertThat(emitters()).hasSize(1);
    }

    @Test
    void subscribe_returnsAnInfiniteTimeoutEmitter() {
        SseEmitter emitter = service.subscribe();

        assertThat(emitter.getTimeout()).isEqualTo(0L);
    }

    @Test
    void broadcast_withNoSubscribers_doesNotThrow() {
        service.broadcast(sampleAlert());
    }

    // Note: SseEmitter's onCompletion callback is wired through Spring MVC's async
    // request infrastructure, and doesn't fire on a bare emitter with no real request
    // attached — that's Spring's own framework wiring, not this class's logic, so it's
    // not re-tested here. The cleanup path that IS this class's own logic (removing an
    // emitter that fails to send) is covered below.

    @Test
    void broadcast_removesEmittersThatFailToSend() throws IOException {
        SseEmitter deadEmitter = mock(SseEmitter.class);
        doThrow(new IOException("client disconnected"))
                .when(deadEmitter).send(any(SseEmitter.SseEventBuilder.class));
        emitters().add(deadEmitter);

        service.broadcast(sampleAlert());

        assertThat(emitters()).doesNotContain(deadEmitter);
    }
}
