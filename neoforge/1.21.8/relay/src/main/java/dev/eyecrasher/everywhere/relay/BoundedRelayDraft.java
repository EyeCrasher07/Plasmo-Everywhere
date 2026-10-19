package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.Wire;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.enums.Opcode;
import org.java_websocket.exceptions.InvalidDataException;
import org.java_websocket.exceptions.LimitExceededException;
import org.java_websocket.framing.Framedata;
import org.java_websocket.framing.FramedataImpl1;
import org.java_websocket.framing.TextFrame;
import org.java_websocket.framing.BinaryFrame;
import org.java_websocket.protocols.Protocol;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;

/** Bound every message fragment before the underlying draft retains its payload. */
final class BoundedRelayDraft extends Draft_6455 {
    static final int MAX_FRAGMENTS=128;
    private int fragments;
    private Opcode messageOpcode;
    private ByteArrayOutputStream message;
    BoundedRelayDraft() {
        super(Collections.emptyList(),List.of(new Protocol("")),Wire.MAX_TEXT);
    }
    @Override public Draft copyInstance() { return new BoundedRelayDraft(); }
    @Override public void reset() { super.reset(); clearMessage(); }
    private void clearMessage() { message=null; messageOpcode=null; fragments=0; }
    @Override public void processFrame(WebSocketImpl socket,Framedata frame) throws InvalidDataException {
        Opcode opcode=frame.getOpcode();
        boolean data=opcode==Opcode.TEXT || opcode==Opcode.BINARY || opcode==Opcode.CONTINUOUS;
        if(!data) { super.processFrame(socket,frame); return; }
        try {
            if((opcode==Opcode.CONTINUOUS)!=(messageOpcode!=null))
                throw new InvalidDataException(1002,"Invalid voice fragment sequence");
            ByteBuffer payload=frame.getPayloadData(); int size=payload.remaining(),previous=message==null?0:message.size();
            if(size>Wire.MAX_TEXT-previous || fragments>=MAX_FRAGMENTS)
                throw new LimitExceededException("Voice message limit",Wire.MAX_TEXT);
            if(message==null && frame.isFin()) { super.processFrame(socket,frame); return; }
            if(message==null) { message=new ByteArrayOutputStream(); messageOpcode=opcode; }
            fragments++;
            byte[] bytes=new byte[size]; payload.duplicate().get(bytes); message.writeBytes(bytes);
            if(frame.isFin()) {
                FramedataImpl1 complete=messageOpcode==Opcode.TEXT?new TextFrame():new BinaryFrame();
                complete.setPayload(ByteBuffer.wrap(message.toByteArray())); complete.isValid();
                clearMessage(); super.processFrame(socket,complete);
            }
        } catch(InvalidDataException | RuntimeException error) { clearMessage(); throw error; }
    }
}
