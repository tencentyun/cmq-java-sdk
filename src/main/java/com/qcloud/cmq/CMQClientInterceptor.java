package com.qcloud.cmq;

import java.util.List;
import java.util.Map;

/**
 * @ClassName CMQClientInterceptor
 * @Description cmq client interceptor
 * @Author hugo
 * @Date 2020/10/13 下午7:32
 * @Version 1.0
 **/
public interface CMQClientInterceptor {

    String intercept(String action, Map<String, String> param, Chain chain) throws Exception ;

    interface Chain {
        String call(String action, Map<String, String> param) throws Exception ;
    }

    class Chains implements Chain {
        private final CMQClient cmqClient;
        private final List<CMQClientInterceptor> interceptorList;

        public Chains(CMQClient cmqClient, List<CMQClientInterceptor> interceptorList) {
            this.cmqClient = cmqClient;
            this.interceptorList = interceptorList;
        }

        @Override
        public String call(String action, Map<String, String> param) throws Exception {
            return new DefaultChain(cmqClient, interceptorList).call(action, param);
        }
    }

    class DefaultChain implements Chain {

        private CMQClient cmqClient;

        private int index = 0;

        private List<CMQClientInterceptor> interceptorList;

        public DefaultChain(CMQClient cmqClient,List<CMQClientInterceptor> interceptorList ) {
            this.cmqClient = cmqClient;
            this.interceptorList = interceptorList;
        }

        @Override
        public String call(String action, Map<String, String> param) throws Exception {
            if (index == interceptorList.size()) {
                return cmqClient.call(action, param);
            } else {
                return interceptorList.get(index++).intercept(action, param, this); // 传this(调度器)用于回调
            }
        }
    }
}
