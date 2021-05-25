package net.qrono.server.data;

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

public interface ForwardingReferenceCounted<T extends ReferenceCounted> extends ReferenceCounted {
  Object ref();

  T self();

  @Override
  default int refCnt() {
    return ReferenceCountUtil.refCnt(ref());
  }

  @Override
  default T retain() {
    ReferenceCountUtil.retain(ref());
    return self();
  }

  @Override
  default T retain(int increment) {
    ReferenceCountUtil.retain(ref(), increment);
    return self();
  }

  @Override
  default T touch() {
    ReferenceCountUtil.touch(ref());
    return self();
  }

  @Override
  default T touch(Object hint) {
    ReferenceCountUtil.touch(ref(), hint);
    return self();
  }

  @Override
  default boolean release() {
    return ReferenceCountUtil.release(ref());
  }

  @Override
  default boolean release(int decrement) {
    return ReferenceCountUtil.release(ref(), decrement);
  }
}
