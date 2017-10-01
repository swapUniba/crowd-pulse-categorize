package com.github.frapontillo.pulse.crowd.categorize;

import com.github.frapontillo.pulse.crowd.data.entity.Category;
import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.crowd.data.entity.Tag;
import com.github.frapontillo.pulse.rx.PulseSubscriber;
import com.github.frapontillo.pulse.spi.IPlugin;
import rx.Observable;
import rx.Subscriber;

import java.util.List;

/**
 * Rx {@link rx.Observable.Operator} for categorizing tags.
 *
 * @author Francesco Pontillo
 */
public abstract class ITagCategorizerOperator implements Observable.Operator<Message, Message> {
    private IPlugin plugin;

    public ITagCategorizerOperator(IPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public Subscriber<? super Message> call(Subscriber<? super Message> subscriber) {
        return new PulseSubscriber<Message>(subscriber) {
            @Override
            public void onNext(Message message) {
                plugin.reportElementAsStarted(message.getId());
                if (message.getTags() != null) {
                    message.getTags().forEach(ITagCategorizerOperator.this::categorizeTag);
                }
                plugin.reportElementAsEnded(message.getId());
                subscriber.onNext(message);
            }

            @Override public void onCompleted() {
                plugin.reportPluginAsCompleted();
                super.onCompleted();
            }

            @Override public void onError(Throwable e) {
                plugin.reportPluginAsErrored();
                super.onError(e);
            }
        };
    }

    /**
     * Actual retrieval of categories from a {@link Tag} happens here.
     * Custom implementations must implement this method and return the appropriate values.
     *
     * @param tag The input {@link Tag} to look for categories into.
     * @return A {@link List} of {@link Category} representing the found categories.
     */
    public abstract List<Category> getCategories(Tag tag);

    /**
     * Retrieves the categories from the input {@link Tag} (if it is not a stop word)
     * and sets them into the object itself in a {@link java.util.Set} structure.
     *
     * @param tag The input {@link Tag} to be enriched with categories.
     * @return The same input {@link Tag}, enriched with categories.
     */
    public Tag categorizeTag(Tag tag) {
        if (!tag.isStopWord()) {
            tag.addCategories(getCategories(tag));
        }
        return tag;
    }

}
