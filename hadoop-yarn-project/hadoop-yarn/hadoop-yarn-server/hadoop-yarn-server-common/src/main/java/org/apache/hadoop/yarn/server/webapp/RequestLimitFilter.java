package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class RequestLimitFilter implements Filter {

  private static final Logger LOG = LoggerFactory.getLogger(RequestLimitFilter.class);

  private int maxRequestsPerMinutePerAPI;

  private int maxRequestsPerMinute;
  private static String MAX_REQUESTS_PER_MINUTE_PER_API = "max-requests-per-minute-per-api";

  private static String MAX_REQUESTS_PER_MINUTE = "max-requests-per-minute";
  private static String ALLOW_USERS = "allow-users";

  private static String WHITE_USER_FROM_REQUEST = "ALLOW-USER";
  private Map<String, RequestLimit> requestsMap;

  private ArrayBlockingQueue<RequestLimit> requestsQueue;

  private static String[] ignorePathPrefix = {"/static/", "/prom"};

  private Set<String> users;

  private static int ONE_MINUTE = 60 * 1000;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    maxRequestsPerMinutePerAPI = 5;
    String maxReqPerMinute = filterConfig.getInitParameter(MAX_REQUESTS_PER_MINUTE_PER_API);
    if (!StringUtils.isNullOrEmpty(maxReqPerMinute)) {
      maxRequestsPerMinutePerAPI = Integer.valueOf(maxReqPerMinute);
    }
    maxRequestsPerMinute = 10;
    String totalReqPerMinute = filterConfig.getInitParameter(MAX_REQUESTS_PER_MINUTE);
    if (!StringUtils.isNullOrEmpty(totalReqPerMinute)) {
      maxRequestsPerMinute = Integer.valueOf(totalReqPerMinute);
    }
    requestsMap = new ConcurrentHashMap<>();
    users = new HashSet<>();
    String whiteUsers = filterConfig.getInitParameter(ALLOW_USERS);
    if (!StringUtils.isNullOrEmpty(whiteUsers)) {
      users.addAll(Arrays.asList(whiteUsers.split(",")));
    }
    requestsQueue = new ArrayBlockingQueue<>(maxRequestsPerMinute);
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
      FilterChain filterChain) throws IOException, ServletException {
    if (servletRequest instanceof HttpServletRequest) {
      HttpServletRequest request = (HttpServletRequest) servletRequest;
      HttpServletResponse response = (HttpServletResponse) servletResponse;
      boolean isSpecifiedUser = isSpecifiedUser(request.getParameter(WHITE_USER_FROM_REQUEST));
      String requestPath = request.getRequestURI();
      boolean ignoreReqPath = ignoreRequestPath(requestPath);
      if (!isSpecifiedUser && !ignoreReqPath) {
        synchronized (RequestLimitFilter.class) {
          try {
            // clean old data if the old request is timeout
            cleanOverTimeData();

            if (requestsQueue.size() >= maxRequestsPerMinute) {
              response.getWriter()
                  .write("Current request count exceeded the Total Allowed Count for Per Minute");
              return;
            }

            RequestLimit requestLimit = requestsMap.get(requestPath);
            if (null == requestLimit) {
              // record per api
              requestLimit = new RequestLimit(requestPath);
              requestsMap.put(requestPath, requestLimit);
            }

            // Per API count check
            if (requestLimit.requestNum >= maxRequestsPerMinutePerAPI) {
              response.getWriter().write("Current API request count exceeded the max limitation");
              return;
            }

            // add to queue
            requestsQueue.put(new RequestLimit(requestPath));
            requestLimit.requestNum++;

            // Log
            if (LOG.isDebugEnabled()) {
              for (String path : requestsMap.keySet()) {
                LOG.debug("request path:" + path + ", count:" + requestsMap.get(path).requestNum);
              }
              LOG.debug("total request count:" + requestsQueue.size());
              LOG.debug("Per Minute Limit for per request:" + maxRequestsPerMinutePerAPI);
              LOG.debug("Per Minute Limit for total request:" + maxRequestsPerMinute);
            }
          } catch (Exception e) {
            LOG.debug("ERROR:", e);
          }
        }
      }
    }
    filterChain.doFilter(servletRequest, servletResponse);
  }

  private void cleanOverTimeData() {
    long currentTime = System.currentTimeMillis();
    if (requestsQueue.size() > 0) {
      Iterator<RequestLimit> it = requestsQueue.iterator();
      while (it.hasNext()) {
        RequestLimit requestLimit = it.next();
        if (currentTime - requestLimit.lastLimitRequestTime < ONE_MINUTE) {
          return;
        }
        it.remove();
        RequestLimit req = requestsMap.get(requestLimit.reqPath);
        if (req != null) {
          req.requestNum--;
          if (req.requestNum <= 0) {
            requestsMap.remove(requestLimit.reqPath);
          }
        }
      }
    }
  }

  private boolean ignoreRequestPath(String requestPath) {
    for (String reqPrefix : ignorePathPrefix) {
      if (requestPath.startsWith(reqPrefix)) {
        return true;
      }
    }
    return false;
  }

  private boolean isSpecifiedUser(String user) {
    if (StringUtils.isNullOrEmpty(user) || !this.users.contains(user)) {
      return false;
    }
    return true;
  }

  @Override
  public void destroy() {

  }

  class RequestLimit {
    int requestNum;
    long lastLimitRequestTime;

    String reqPath;

    RequestLimit(String requestPath) {
      reqPath = requestPath;
      requestNum = 0;
      lastLimitRequestTime = System.currentTimeMillis();
    }
  }
}
