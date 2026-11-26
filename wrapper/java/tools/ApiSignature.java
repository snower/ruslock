import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Emits the externally visible Java API of a jar in a stable, diffable form.
 *
 * <p>Only public and protected classes, constructors, methods and fields are
 * emitted. Synthetic and bridge members are filtered out. Generic types and
 * declared exceptions are preserved so a wrapper can be compared with the
 * Jaslock baseline jar it replaces.
 *
 * <p>Usage: {@code java ApiSignature <jar> <output>}
 */
public final class ApiSignature {
    private ApiSignature() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: ApiSignature <jar> <output>");
            System.exit(2);
        }

        File jar = new File(args[0]);
        if (!jar.isFile()) {
            System.err.println("not a file: " + jar);
            System.exit(2);
        }

        List<String> classNames = new ArrayList<String>();
        JarFile jarFile = new JarFile(jar);
        try {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();
                if (name.endsWith(".class") && !name.startsWith("META-INF/")) {
                    classNames.add(name.substring(0, name.length() - ".class".length()).replace('/', '.'));
                }
            }
        } finally {
            jarFile.close();
        }
        Collections.sort(classNames);

        URLClassLoader loader = new URLClassLoader(new URL[]{jar.toURI().toURL()},
                ApiSignature.class.getClassLoader());
        List<String> lines = new ArrayList<String>();
        for (String className : classNames) {
            Class<?> type;
            try {
                type = Class.forName(className, false, loader);
            } catch (Throwable t) {
                lines.add("UNLOADABLE " + className + " : " + t.getClass().getName());
                lines.add("");
                continue;
            }

            if (type.isSynthetic() || type.isAnonymousClass() || type.isLocalClass()) {
                continue;
            }
            int classModifiers = type.getModifiers();
            if (!isVisible(classModifiers)) {
                continue;
            }

            lines.add(describeType(type));
            TypeVariable<?>[] typeParameters = type.getTypeParameters();
            if (typeParameters.length > 0) {
                lines.add("  TYPE-PARAMS " + typeParameters(typeParameters));
            }
            Type superclass = type.getGenericSuperclass();
            if (superclass != null) {
                lines.add("  SUPER " + typeName(superclass));
            }
            Type[] interfaces = type.getGenericInterfaces();
            for (Type interfaceType : interfaces) {
                lines.add("  IMPLEMENTS " + typeName(interfaceType));
            }

            List<String> members = new ArrayList<String>();
            for (Constructor<?> constructor : type.getDeclaredConstructors()) {
                int modifiers = constructor.getModifiers();
                if (!isVisible(modifiers) || constructor.isSynthetic()) {
                    continue;
                }
                members.add("  CTOR " + modifiers(modifiers) + type.getSimpleName()
                        + parameters(constructor.getGenericParameterTypes(), constructor.isVarArgs())
                        + exceptions(constructor.getGenericExceptionTypes()));
            }
            for (Method method : type.getDeclaredMethods()) {
                int modifiers = method.getModifiers();
                if (!isVisible(modifiers) || method.isSynthetic() || method.isBridge()) {
                    continue;
                }
                members.add("  METHOD " + modifiers(modifiers)
                        + typeParameters(method.getTypeParameters())
                        + typeName(method.getGenericReturnType()) + " " + method.getName()
                        + parameters(method.getGenericParameterTypes(), method.isVarArgs())
                        + exceptions(method.getGenericExceptionTypes()));
            }
            for (Field field : type.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                if (!isVisible(modifiers) || field.isSynthetic()) {
                    continue;
                }
                members.add("  FIELD " + modifiers(modifiers) + typeName(field.getGenericType())
                        + " " + field.getName());
            }
            Collections.sort(members);
            lines.addAll(members);
            lines.add("");
        }
        loader.close();

        Writer writer = new OutputStreamWriter(new FileOutputStream(args[1]), "UTF-8");
        try {
            for (String line : lines) {
                writer.write(line);
                writer.write('\n');
            }
        } finally {
            writer.close();
        }
    }

    private static boolean isVisible(int modifiers) {
        return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
    }

    private static String describeType(Class<?> type) {
        String kind;
        if (type.isAnnotation()) {
            kind = "@interface";
        } else if (type.isInterface()) {
            kind = "interface";
        } else if (type.isEnum()) {
            kind = "enum";
        } else {
            kind = "class";
        }
        return "TYPE " + modifiers(type.getModifiers() & ~Modifier.INTERFACE) + kind + " " + type.getName();
    }

    private static String modifiers(int modifiers) {
        String value = Modifier.toString(modifiers);
        return value.isEmpty() ? "" : value + " ";
    }

    private static String typeParameters(TypeVariable<?>[] variables) {
        if (variables.length == 0) {
            return "";
        }
        StringBuilder result = new StringBuilder("<");
        for (int i = 0; i < variables.length; i++) {
            if (i > 0) {
                result.append(", ");
            }
            result.append(variables[i].getName());
            Type[] bounds = variables[i].getBounds();
            if (bounds.length > 0 && !(bounds.length == 1 && bounds[0] == Object.class)) {
                result.append(" extends ");
                for (int j = 0; j < bounds.length; j++) {
                    if (j > 0) {
                        result.append(" & ");
                    }
                    result.append(typeName(bounds[j]));
                }
            }
        }
        return result.append("> ").toString();
    }

    private static String typeName(Type type) {
        return type.getTypeName();
    }

    private static String parameters(Type[] parameterTypes, boolean varArgs) {
        if (parameterTypes.length == 0) {
            return "()";
        }
        StringBuilder result = new StringBuilder("(");
        for (int i = 0; i < parameterTypes.length; i++) {
            if (i > 0) {
                result.append(", ");
            }
            String parameter = typeName(parameterTypes[i]);
            if (varArgs && i == parameterTypes.length - 1 && parameter.endsWith("[]")) {
                parameter = parameter.substring(0, parameter.length() - 2) + "...";
            }
            result.append(parameter);
        }
        return result.append(")").toString();
    }

    private static String exceptions(Type[] exceptionTypes) {
        if (exceptionTypes.length == 0) {
            return "";
        }
        String[] names = new String[exceptionTypes.length];
        for (int i = 0; i < exceptionTypes.length; i++) {
            names[i] = typeName(exceptionTypes[i]);
        }
        Arrays.sort(names);
        StringBuilder result = new StringBuilder(" throws ");
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                result.append(", ");
            }
            result.append(names[i]);
        }
        return result.toString();
    }
}
