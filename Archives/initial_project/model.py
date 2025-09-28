# Core
import pandas as pd
import numpy as np
import re
from datetime import datetime
import os

# TensorFlow + Keras
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import models, layers
from tensorflow.keras.callbacks import EarlyStopping
import tensorflow.keras.backend as K
from tensorflow.keras.saving import register_keras_serializable
from tensorflow.keras.models import load_model

# Scikit-learn tools (still useful for prep)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.utils import class_weight

# For managing categorical features more flexibly
import category_encoders as ce

# For visualizing training
import matplotlib.pyplot as plt


#try xgboost



#Functions
# Function to calculate the focal loss
@register_keras_serializable()
def focal_loss(gamma=2., alpha=0.25):
    def focal_loss_fixed(y_true, y_pred):
        y_true = K.cast(y_true, tf.float32)
        pt = tf.where(tf.equal(y_true, 1), y_pred, 1 - y_pred)
        return -K.mean(alpha * K.pow(1. - pt, gamma) * K.log(K.clip(pt, 1e-8, 1. - 1e-8)))
    return focal_loss_fixed


#load the data
df = pd.read_csv('data/cleanMD.csv')

#Normalize the data

num_cols = ['tax', 'paid', 'txbl', 'examt', 'owner_interest', 'rate', 'acres', 'operator_size', 'num_properties', 'latefee', 'latepaid']
scaler = StandardScaler()
df[num_cols] = scaler.fit_transform(df[num_cols])

#Assigns the target variable and the features

id_cols = df[['yr', 'geoid', 'ownerid', 'id', 'rrc_number', 'next_year_ownerid']]
X = df.drop(columns=['changed_owner', 'geoid', 'ownerid', 'id', 'rrc_number', 'next_year_ownerid'])
y = df['changed_owner']

#Train/Test Split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"X_train shape: {X_train.shape}")
print(f"y_train shape: {y_train.shape}")
print("Class distribution:\n", y_train.value_counts(normalize=True))


#Baseline model
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

logreg = LogisticRegression(class_weight='balanced', max_iter=1000)
logreg.fit(X_train, y_train)
y_prob = logreg.predict_proba(X_test)[:, 1]
print("AUC:", roc_auc_score(y_test, y_prob))


# Compute class weights
class_weights = class_weight.compute_class_weight(
    class_weight='balanced',
    classes=np.array([0, 1]),
    y=y_train
)

# ==== Callbacks ====
early_stop = tf.keras.callbacks.EarlyStopping(
    monitor='val_auc',         # Better than val_loss for imbalanced data
    mode='max',
    patience=5,                # Wait a few epochs for potential improvement
    restore_best_weights=True
)


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
model_path = f"saved_model/landman_model_{timestamp}.keras"

model_checkpoint = tf.keras.callbacks.ModelCheckpoint(
    filepath=model_path,
    monitor='val_loss',
    save_best_only=True
)

#Modeling

model = models.Sequential([
    layers.Input(shape=(X.shape[1],)),
    layers.Dense(64, activation='relu'),
    layers.Dense(32, activation='relu'),
    layers.Dense(1, activation='sigmoid')
])

model.compile(
    optimizer = tf.keras.optimizers.Adam(learning_rate=0.001),
    loss='binary_crossentropy', 
    metrics=[
        tf.keras.metrics.Recall(name='recall'),
        tf.keras.metrics.Precision(name='precision'),
        tf.keras.metrics.AUC(name='auc')
    ]
)

history=model.fit(
    X_train, y_train,
    epochs=100,
    batch_size=256,
    validation_split=0.2,
    callbacks=[early_stop, model_checkpoint],
    class_weight={0: class_weights[0], 1: class_weights[1]}
)

model = load_model(model_path, custom_objects={"focal_loss_fixed": focal_loss(gamma=2., alpha=0.25)})

# ==== Evaluate ====
results = model.evaluate(X_test, y_test)
print("\n📊 Final Evaluation Metrics:")
for name, value in zip(model.metrics_names, results):
    print(f"{name.capitalize()}: {value:.4f}")




# ==== Plot loss and metrics ====
plt.figure(figsize=(14, 6))

# Plot Loss
plt.subplot(1, 2, 1)
plt.plot(history.history['loss'], label='Train Loss', linewidth=2)
plt.plot(history.history['val_loss'], label='Validation Loss', linewidth=2)
plt.title('Loss Over Epochs')
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.legend()
plt.grid(True)

# Plot Metrics (excluding loss)
metric_keys = [k for k in history.history.keys() if not k.startswith('loss')]
if metric_keys:
    plt.subplot(1, 2, 2)
    for k in metric_keys:
        plt.plot(history.history[k], label=k.replace('_', ' ').title(), linewidth=2)
    plt.title('Validation Metrics Over Epochs')
    plt.xlabel('Epoch')
    plt.ylabel('Score')
    plt.legend()
    plt.grid(True)

plt.suptitle('Model Training Summary', fontsize=16)
plt.tight_layout()
plt.show()

#read the sample data
df_sample = pd.read_csv('data/2024_wells.csv')


# 2. Save the ID columns for later
id_cols = df_sample[['geoid', 'ownerid', 'id', 'rrc_number', 'yr']].copy()

# 3. Select and scale the features
X_2024 = df_sample.drop(columns=[
    'changed_owner', 'geoid', 'ownerid', 'id',
    'rrc_number', 'next_year_ownerid'
], errors='ignore')

X_2024[num_cols] = scaler.transform(X_2024[num_cols])

# 4. Run prediction
pred_probs = model.predict(X_2024, verbose=0)

# 5. Reattach IDs and predictions
results_2024 = id_cols.copy()
results_2024['sell_probability'] = pred_probs

results_full = pd.concat([df_sample.reset_index(drop=True), results_2024['sell_probability']], axis=1)


# 6. Sort and view top leads
top_leads = results_full.sort_values(by='sell_probability', ascending=False)
print(top_leads.head(20))

# 7. Optional: save to CSV
top_leads.to_csv("top_2024_leads.csv", index=False)