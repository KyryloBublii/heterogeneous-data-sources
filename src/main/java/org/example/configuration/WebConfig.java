package org.example.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/dashboard").setViewName("forward:/dashboard.html");
        registry.addViewController("/sources").setViewName("forward:/sources.html");
        registry.addViewController("/datasets").setViewName("forward:/datasets.html");
        registry.addViewController("/dataset").setViewName("forward:/dataset.html");
        registry.addViewController("/explore").setViewName("forward:/explore.html");
        registry.addViewController("/profile").setViewName("forward:/profile.html");
    }
}
