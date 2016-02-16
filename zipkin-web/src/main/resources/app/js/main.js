import {compose, registry, advice, logger as withLogging, debug} from 'flight';
import $ from 'jquery';
import crossroads from 'crossroads';
import queryString from 'query-string';
import initializeDefault from './page/default';
import initializeTrace from './page/trace';
import initializeDependency from './page/dependency';

Array.prototype.remove = function(from, to) {
  var rest = this.slice((to || from) + 1 || this.length);
  this.length = from < 0 ? this.length + from : from;
  return this.push.apply(this, rest);
};

debug.enable(true);
compose.mixin(registry, [advice.withAdvice]);

crossroads.addRoute('', initializeDefault);
crossroads.addRoute('traces/{id}', initializeTrace);
crossroads.addRoute('dependency', initializeDependency);
crossroads.parse(window.location.pathname);
