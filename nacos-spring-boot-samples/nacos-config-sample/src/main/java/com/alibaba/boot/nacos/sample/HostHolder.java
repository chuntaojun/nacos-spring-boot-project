package com.alibaba.boot.nacos.sample;

import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

/**
 * @author <a href="mailto:liaochuntao@youzan.com">liaochuntao</a>
 * @Created at 2020/1/9 10:40 下午
 */
@RequestScope(proxyMode = ScopedProxyMode.TARGET_CLASS)
@Component
public class HostHolder {

    private long userId;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }
}